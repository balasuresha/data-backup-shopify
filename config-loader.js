const { SecretClient } = require("@azure/keyvault-secrets");
const { DefaultAzureCredential, ManagedIdentityCredential } = require("@azure/identity");
const fs = require("fs").promises;
const path = require("path");

// Enhanced configuration loader with Azure Key Vault support
async function loadConfig() {
  try {
    let config = {};
    const isLocalhost = process.env.NODE_ENV === 'development' || 
                       process.env.AZURE_FUNCTIONS_ENVIRONMENT === 'Development' ||
                       !process.env.WEBSITE_SITE_NAME; // Azure Functions sets this in cloud

    console.log("Environment detection:", {
      nodeEnv: process.env.NODE_ENV,
      azureFunctionsEnv: process.env.AZURE_FUNCTIONS_ENVIRONMENT,
      websiteSiteName: process.env.WEBSITE_SITE_NAME,
      isLocalhost
    });

    if (isLocalhost) {
      console.log("Running locally - loading from local.settings.json");
      config = await loadLocalConfig();
    } else {
      console.log("Running in Azure - loading from Key Vault with fallback to environment variables");
      config = await loadFromKeyVault();
    }

    // Validate required configuration for both Shopify and Table functions
    const requiredVars = [
      "SHOPIFY_STORE_URL", 
      "SHOPIFY_ACCESS_TOKEN",
      "AZURE_STORAGE_CONNECTION_STRING",
      "BLOB_CONTAINER_NAME",
      // Table-specific configuration
      "STORE_EXTRACTION_LOG_TABLE",
      "STORE_EXTRACTION_DETAILS_TABLE"
    ];
    
    const missingVars = requiredVars.filter((varName) => !config[varName]);

    if (missingVars.length > 0) {
      console.warn(
        "Some configuration values are missing: " + missingVars.join(", ") + 
        "\nEnvironment: " + (isLocalhost ? "localhost" : "Azure") +
        "\nThis may be expected if not all functions are being used."
      );
      // Don't throw error - some functions may not need all config
    }

    console.log("Configuration loaded successfully", {
      environment: isLocalhost ? "localhost" : "Azure",
      configuredKeys: Object.keys(config).filter(key => !key.includes('SECRET') && !key.includes('TOKEN') && !key.includes('KEY')),
      hasTableConfig: !!(config.STORE_EXTRACTION_LOG_TABLE && config.STORE_EXTRACTION_DETAILS_TABLE),
      hasShopifyConfig: !!(config.SHOPIFY_STORE_URL && config.SHOPIFY_ACCESS_TOKEN),
      hasStorageConfig: !!config.AZURE_STORAGE_CONNECTION_STRING
    });

    return config;
    
  } catch (error) {
    console.error("Error loading configuration:", error.message);
    throw error;
  }
}

// Load configuration from local.settings.json for development
async function loadLocalConfig() {
  let config = {};
  let localSettings = {};

  const possiblePaths = [
    path.join(process.cwd(), "local.settings.json"),
    path.join(__dirname, "..", "local.settings.json"),
    path.join(__dirname, "local.settings.json"),
  ];

  // Try to find and load local.settings.json
  for (const testPath of possiblePaths) {
    try {
      await fs.access(testPath);
      const configFile = await fs.readFile(testPath, "utf8");
      const localConfig = JSON.parse(configFile);
      localSettings = localConfig.Values || {};
      console.log("📁 local.settings.json found at:", testPath);
      break;
    } catch (error) {
      // Continue to next path
    }
  }

  // Merge environment variables and local settings (env vars take precedence)
  const allKeys = new Set([
    ...Object.keys(localSettings),
    ...Object.keys(process.env),
  ]);

  for (const key of allKeys) {
    if (process.env[key] !== undefined && process.env[key] !== "") {
      config[key] = process.env[key];
      console.log("Using environment variable for:", key);
    } else if (localSettings[key] !== undefined) {
      config[key] = localSettings[key];
      console.log("Using local.settings.json for:", key);
    }
  }

  return config;
}

// Load configuration from Azure Key Vault for production with fallback
async function loadFromKeyVault() {
  const config = {};
  
  try {
    // Get Key Vault URL from environment variable
    const keyVaultUrl = process.env.AZURE_KEY_VAULT_URL;
    
    if (!keyVaultUrl) {
      console.warn("AZURE_KEY_VAULT_URL not found, falling back to environment variables");
      return loadFromEnvironmentVariables();
    }

    console.log("Connecting to Azure Key Vault:", keyVaultUrl);

    // Use DefaultAzureCredential which tries multiple authentication methods
    const credential = new DefaultAzureCredential();
    const client = new SecretClient(keyVaultUrl, credential);

    // Enhanced secret mappings to include table configuration
    const secretMappings = {
      // Shopify configuration
      "shopify-store-url": "SHOPIFY_STORE_URL",
      "shopify-access-token": "SHOPIFY_ACCESS_TOKEN", 
      "shopify-api-version": "SHOPIFY_API_VERSION",
      
      // Azure Storage configuration
      "azure-storage-connection-string": "AZURE_STORAGE_CONNECTION_STRING",
      "azure-storage-account-name": "AZURE_STORAGE_ACCOUNT_NAME",
      "azure-storage-account-key": "AZURE_STORAGE_ACCOUNT_KEY",
      
      // Application configuration
      "blob-container-name": "BLOB_CONTAINER_NAME",
      "extraction-tag": "EXTRACTION_TAG",
      "max-retry-attempts": "MAX_RETRY_ATTEMPTS",
      "retry-delay-ms": "RETRY_DELAY_MS",
      "max-concurrent-batches": "MAX_CONCURRENT_BATCHES",
      "log-level": "LOG_LEVEL",
      
      // Table-specific configuration
      "store-extraction-log-table": "STORE_EXTRACTION_LOG_TABLE",
      "store-extraction-details-table": "STORE_EXTRACTION_DETAILS_TABLE"
    };

    console.log("Retrieving secrets from Key Vault...");

    // Retrieve secrets concurrently for better performance
    const secretPromises = Object.entries(secretMappings).map(async ([secretName, configKey]) => {
      try {
        console.log(`Fetching secret: ${secretName}`);
        const secret = await client.getSecret(secretName);
        
        if (secret.value) {
          config[configKey] = secret.value;
          console.log(`Successfully retrieved: ${configKey}`);
          return { key: configKey, success: true };
        } else {
          console.warn(`Secret ${secretName} exists but has no value`);
          return { key: configKey, success: false, reason: 'no_value' };
        }
      } catch (error) {
        if (error.statusCode === 404) {
          console.warn(`Secret not found in Key Vault: ${secretName}`);
          return { key: configKey, success: false, reason: 'not_found' };
        } else {
          console.error(`Error retrieving secret ${secretName}:`, error.message);
          return { key: configKey, success: false, reason: 'error', error: error.message };
        }
      }
    });

    const results = await Promise.all(secretPromises);
    
    // Log summary of retrieved secrets
    const successful = results.filter(r => r.success);
    const failed = results.filter(r => !r.success);
    
    console.log("Key Vault retrieval summary:", {
      successful: successful.length,
      failed: failed.length,
      successfulKeys: successful.map(r => r.key),
      failedKeys: failed.map(r => ({ key: r.key, reason: r.reason }))
    });

    // Fallback to environment variables for missing secrets
    const missingKeys = failed.map(r => r.key);
    if (missingKeys.length > 0) {
      console.log("Attempting to load missing configuration from environment variables:", missingKeys);
      
      missingKeys.forEach(key => {
        if (process.env[key] !== undefined && process.env[key] !== "") {
          config[key] = process.env[key];
          console.log(`Using environment variable fallback for: ${key}`);
        }
      });
    }

    // Also check for any environment variables that might override Key Vault values
    const allConfigKeys = Object.values(secretMappings);
    const envOverrides = allConfigKeys.filter(key => 
      process.env[key] !== undefined && process.env[key] !== "" && config[key] !== process.env[key]
    );

    if (envOverrides.length > 0) {
      console.log("Environment variable overrides found:", envOverrides);
      envOverrides.forEach(key => {
        config[key] = process.env[key];
        console.log(`Using environment override for: ${key}`);
      });
    }

    return config;

  } catch (error) {
    console.error("Failed to load configuration from Key Vault:", error.message);
    
    // If Key Vault fails completely, fall back to environment variables
    console.log("Attempting complete fallback to environment variables...");
    return loadFromEnvironmentVariables();
  }
}

// Fallback function to load from environment variables only
function loadFromEnvironmentVariables() {
  console.log("Loading configuration from environment variables...");
  
  const config = {};
  const configKeys = [
    // Shopify configuration
    "SHOPIFY_STORE_URL", 
    "SHOPIFY_ACCESS_TOKEN",
    "SHOPIFY_API_VERSION",
    
    // Azure Storage configuration
    "AZURE_STORAGE_CONNECTION_STRING",
    "AZURE_STORAGE_ACCOUNT_NAME",
    "AZURE_STORAGE_ACCOUNT_KEY",
    
    // Application configuration
    "BLOB_CONTAINER_NAME",
    "EXTRACTION_TAG",
    "MAX_RETRY_ATTEMPTS", 
    "RETRY_DELAY_MS",
    "MAX_CONCURRENT_BATCHES",
    "LOG_LEVEL",
    
    // Table-specific configuration
    "STORE_EXTRACTION_LOG_TABLE",
    "STORE_EXTRACTION_DETAILS_TABLE"
  ];

  let loadedCount = 0;
  configKeys.forEach(key => {
    if (process.env[key] !== undefined && process.env[key] !== "") {
      config[key] = process.env[key];
      console.log(`Loaded from environment: ${key}`);
      loadedCount++;
    }
  });

  console.log(`Environment variable fallback completed: ${loadedCount}/${configKeys.length} variables loaded`);
  
  if (loadedCount === 0) {
    throw new Error("No configuration found in environment variables");
  }
  
  return config;
}

// Helper function to set up Key Vault secrets (for initial setup)
async function setupKeyVaultSecrets(keyVaultUrl, secretValues) {
  console.log("Setting up Key Vault secrets...");
  
  try {
    const credential = new DefaultAzureCredential();
    const client = new SecretClient(keyVaultUrl, credential);

    const secretMappings = {
      // Shopify configuration
      "shopify-store-url": "SHOPIFY_STORE_URL",
      "shopify-access-token": "SHOPIFY_ACCESS_TOKEN",
      "shopify-api-version": "SHOPIFY_API_VERSION", 
      
      // Azure Storage configuration
      "azure-storage-connection-string": "AZURE_STORAGE_CONNECTION_STRING",
      "azure-storage-account-name": "AZURE_STORAGE_ACCOUNT_NAME",
      "azure-storage-account-key": "AZURE_STORAGE_ACCOUNT_KEY",
      
      // Application configuration
      "blob-container-name": "BLOB_CONTAINER_NAME",
      "extraction-tag": "EXTRACTION_TAG",
      "max-retry-attempts": "MAX_RETRY_ATTEMPTS",
      "retry-delay-ms": "RETRY_DELAY_MS", 
      "max-concurrent-batches": "MAX_CONCURRENT_BATCHES",
      "log-level": "LOG_LEVEL",
      
      // Table-specific configuration
      "store-extraction-log-table": "STORE_EXTRACTION_LOG_TABLE",
      "store-extraction-details-table": "STORE_EXTRACTION_DETAILS_TABLE"
    };

    for (const [secretName, configKey] of Object.entries(secretMappings)) {
      if (secretValues[configKey]) {
        try {
          await client.setSecret(secretName, secretValues[configKey]);
          console.log(`Set secret: ${secretName}`);
        } catch (error) {
          console.error(`Failed to set secret ${secretName}:`, error.message);
        }
      }
    }

    console.log("Key Vault setup completed");
    
  } catch (error) {
    console.error("Key Vault setup failed:", error.message);
    throw error;
  }
}

module.exports = { 
  loadConfig, 
  loadLocalConfig, 
  loadFromKeyVault, 
  loadFromEnvironmentVariables,
  setupKeyVaultSecrets 
};
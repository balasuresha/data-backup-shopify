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
      console.log("Running in Azure - loading from Key Vault");
      config = await loadFromKeyVault();
    }

    // Validate required configuration
    const requiredVars = [
      "SHOPIFY_STORE_URL", 
      "SHOPIFY_ACCESS_TOKEN",
      "AZURE_STORAGE_CONNECTION_STRING",
      "BLOB_CONTAINER_NAME"
    ];
    
    const missingVars = requiredVars.filter((varName) => !config[varName]);

    if (missingVars.length > 0) {
      throw new Error(
        "Missing required configuration values: " + missingVars.join(", ") + 
        "\nEnvironment: " + (isLocalhost ? "localhost" : "Azure")
      );
    }

    console.log("Configuration loaded successfully", {
      environment: isLocalhost ? "localhost" : "Azure",
      configuredKeys: Object.keys(config).filter(key => !key.includes('SECRET') && !key.includes('TOKEN')),
      requiredVarsPresent: requiredVars.every(varName => config[varName])
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

// Load configuration from Azure Key Vault for production
async function loadFromKeyVault() {
  const config = {};
  
  try {
    // Get Key Vault URL from environment variable
    const keyVaultUrl = process.env.AZURE_KEY_VAULT_URL;
    
    if (!keyVaultUrl) {
      throw new Error("AZURE_KEY_VAULT_URL environment variable is required when running in Azure");
    }

    console.log("Connecting to Azure Key Vault:", keyVaultUrl);

    // Use DefaultAzureCredential which tries multiple authentication methods
    // In Azure Functions, this will use Managed Identity
    const credential = new DefaultAzureCredential();
    const client = new SecretClient(keyVaultUrl, credential);

    // Define the secrets to retrieve from Key Vault
    // Key Vault secret names should use hyphens instead of underscores
    const secretMappings = {
      "shopify-store-url": "SHOPIFY_STORE_URL",
      "shopify-access-token": "SHOPIFY_ACCESS_TOKEN", 
      "shopify-api-version": "SHOPIFY_API_VERSION",
      "azure-storage-connection-string": "AZURE_STORAGE_CONNECTION_STRING",
      "blob-container-name": "BLOB_CONTAINER_NAME",
      "extraction-tag": "EXTRACTION_TAG",
      "max-retry-attempts": "MAX_RETRY_ATTEMPTS",
      "retry-delay-ms": "RETRY_DELAY_MS",
      "max-concurrent-batches": "MAX_CONCURRENT_BATCHES",
      "log-level": "LOG_LEVEL"
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

    // Also check for any environment variables that might override Key Vault values
    // This allows for runtime overrides in Azure if needed
    const envOverrides = Object.values(secretMappings).filter(key => 
      process.env[key] !== undefined && process.env[key] !== ""
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
    
    // If Key Vault fails, try to fall back to environment variables
    console.log("Attempting fallback to environment variables...");
    
    const fallbackConfig = {};
    const requiredKeys = [
      "SHOPIFY_STORE_URL", 
      "SHOPIFY_ACCESS_TOKEN",
      "AZURE_STORAGE_CONNECTION_STRING",
      "BLOB_CONTAINER_NAME",
      "SHOPIFY_API_VERSION",
      "EXTRACTION_TAG",
      "MAX_RETRY_ATTEMPTS", 
      "RETRY_DELAY_MS",
      "MAX_CONCURRENT_BATCHES",
      "LOG_LEVEL"
    ];

    let fallbackSuccess = false;
    requiredKeys.forEach(key => {
      if (process.env[key]) {
        fallbackConfig[key] = process.env[key];
        fallbackSuccess = true;
      }
    });

    if (fallbackSuccess) {
      console.log("Fallback to environment variables successful");
      return fallbackConfig;
    } else {
      throw new Error(`Key Vault failed and no environment variable fallback available: ${error.message}`);
    }
  }
}

// Helper function to set up Key Vault secrets (for initial setup)
async function setupKeyVaultSecrets(keyVaultUrl, secretValues) {
  console.log("Setting up Key Vault secrets...");
  
  try {
    const credential = new DefaultAzureCredential();
    const client = new SecretClient(keyVaultUrl, credential);

    const secretMappings = {
      "shopify-store-url": "SHOPIFY_STORE_URL",
      "shopify-access-token": "SHOPIFY_ACCESS_TOKEN",
      "shopify-api-version": "SHOPIFY_API_VERSION", 
      "azure-storage-connection-string": "AZURE_STORAGE_CONNECTION_STRING",
      "blob-container-name": "BLOB_CONTAINER_NAME",
      "extraction-tag": "EXTRACTION_TAG",
      "max-retry-attempts": "MAX_RETRY_ATTEMPTS",
      "retry-delay-ms": "RETRY_DELAY_MS", 
      "max-concurrent-batches": "MAX_CONCURRENT_BATCHES",
      "log-level": "LOG_LEVEL"
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
  setupKeyVaultSecrets 
};
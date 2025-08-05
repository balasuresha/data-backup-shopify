const { app } = require('@azure/functions');
const axios = require('axios');
const { BlobServiceClient } = require('@azure/storage-blob');
const { SecretClient } = require('@azure/keyvault-secrets');
const { DefaultAzureCredential } = require('@azure/identity');
const path = require('path');
const fs = require('fs');

// Configuration constants
const CONFIG = {
    SHOPIFY_API_VERSION: '2025-04',
    GRAPHQL_TIMEOUT: parseInt(process.env.GRAPHQL_TIMEOUT) || 30000,
    DOWNLOAD_TIMEOUT: parseInt(process.env.DOWNLOAD_TIMEOUT) || 120000,
    MAX_POLLING_ATTEMPTS: parseInt(process.env.MAX_POLLING_ATTEMPTS) || 30,
    POLLING_INTERVAL: parseInt(process.env.POLLING_INTERVAL) || 10000,
    DEFAULT_CONTAINER_NAME: 'shopify-data'
};

// Load configuration safely with Key Vault support
async function loadConfig() {
    try {
        let config = {};
        let localSettings = {};
        let keyVaultClient = null;
        
        // Try to initialize Key Vault client if URL is provided
        const keyVaultUrl = process.env.KEY_VAULT_URL || process.env.AZURE_KEY_VAULT_URL;
        
        if (keyVaultUrl) {
            try {
                console.log('Initializing Key Vault client for:', keyVaultUrl);
                const credential = new DefaultAzureCredential();
                keyVaultClient = new SecretClient(keyVaultUrl, credential);
                console.log('Key Vault client initialized successfully');
            } catch (keyVaultError) {
                console.warn('Failed to initialize Key Vault client:', keyVaultError.message);
                console.warn('Falling back to environment variables and local settings');
            }
        } else {
            console.log('No Key Vault URL provided, using environment variables and local settings');
        }

        // Load local.settings.json if available
        const possiblePaths = [
            path.join(process.cwd(), 'local.settings.json'),
            path.join(__dirname, '..', 'local.settings.json'),
            path.join(__dirname, 'local.settings.json')
        ];
       
        for (const testPath of possiblePaths) {
            try {
                fs.accessSync(testPath);
                const configFile = fs.readFileSync(testPath, 'utf8');
                const localConfig = JSON.parse(configFile);
                localSettings = localConfig.Values || {};
                console.log('local.settings.json found at: ' + testPath);
                break;
            } catch {}
        }
        
        // Define required configuration keys and their Key Vault secret names
        const configMapping = {
            'SHOPIFY_STORE_URL': 'shopify-store-url',
            'SHOPIFY_ACCESS_TOKEN': 'shopify-access-token',
            'AZURE_STORAGE_CONNECTION_STRING': 'azure-storage-connection-string',
            'STORAGE_CONTAINER_NAME': 'storage-container-name'
        };

        // Get all possible keys from environment, local settings, and config mapping
        const allKeys = new Set([
            ...Object.keys(localSettings), 
            ...Object.keys(process.env),
            ...Object.keys(configMapping)
        ]);
        
        // Load configuration values in priority order: Key Vault > Environment Variables > Local Settings
        for (const key of allKeys) {
            let valueFound = false;
            
            // 1. Try Key Vault first (if available)
            if (keyVaultClient && configMapping[key]) {
                try {
                    const secretName = configMapping[key];
                    console.log(`Attempting to retrieve secret: ${secretName} for key: ${key}`);
                    const secret = await keyVaultClient.getSecret(secretName);
                    if (secret && secret.value) {
                        config[key] = secret.value;
                        console.log(`Using Key Vault value for: ${key}`);
                        valueFound = true;
                    }
                } catch (keyVaultError) {
                    // Log the error but continue to fallback options
                    console.warn(`Failed to retrieve ${configMapping[key]} from Key Vault:`, keyVaultError.message);
                }
            }
            
            // 2. Try environment variables if Key Vault didn't provide a value
            if (!valueFound && process.env[key] !== undefined && process.env[key] !== '') {
                config[key] = process.env[key];
                console.log(`Using environment variable for: ${key}`);
                valueFound = true;
            }
            
            // 3. Try local settings as final fallback
            if (!valueFound && localSettings[key] !== undefined) {
                config[key] = localSettings[key];
                console.log(`Using local.settings.json for: ${key}`);
                valueFound = true;
            }
        }
        
        // Validate required configuration
        const requiredVars = ['SHOPIFY_STORE_URL', 'SHOPIFY_ACCESS_TOKEN', 'AZURE_STORAGE_CONNECTION_STRING'];
        const missingVars = requiredVars.filter(varName => !config[varName]);
        
        if (missingVars.length > 0) {
            const errorMessage = `Missing required configuration values: ${missingVars.join(', ')}. ` +
                `Please ensure these are configured in Key Vault (${keyVaultUrl ? 'available' : 'not configured'}), ` +
                `environment variables, or local.settings.json.`;
            throw new Error(errorMessage);
        }
        
        console.log('Configuration loaded successfully');
        console.log('Configuration sources used:', {
            keyVault: keyVaultUrl ? 'Available' : 'Not configured',
            environmentVariables: 'Available',
            localSettings: Object.keys(localSettings).length > 0 ? 'Available' : 'Not found'
        });
        
        return config;
    } catch (error) {
        console.error('Error loading configuration:', error.message);
        throw error;
    }
}

// Utility function for exponential backoff retry
async function retryWithExponentialBackoff(operation, maxRetries = 5, baseDelay = 1000, maxDelay = 30000, backoffMultiplier = 2) {
    let lastError;
    
    for (let attempt = 0; attempt < maxRetries; attempt++) {
        try {
            return await operation();
        } catch (error) {
            lastError = error;
            
            // Don't retry on certain types of errors
            if (error.code === 'ENOTFOUND' || error.status === 404 || error.status === 401 || error.status === 403) {
                throw error;
            }
            
            if (attempt === maxRetries - 1) {
                throw error;
            }
            
            // Calculate delay with exponential backoff and jitter
            const exponentialDelay = Math.min(baseDelay * Math.pow(backoffMultiplier, attempt), maxDelay);
            const jitter = Math.random() * 0.1 * exponentialDelay; // Add up to 10% jitter
            const delay = exponentialDelay + jitter;
            
            console.log(`Attempt ${attempt + 1} failed: ${error.message}. Retrying in ${Math.round(delay)}ms...`);
            await new Promise(resolve => setTimeout(resolve, delay));
        }
    }
    
    throw lastError;
}

// Enhanced axios function with timeout and retry
async function axiosWithRetry(url, options = {}, timeout = 60000) {
    const axiosConfig = {
        url,
        timeout,
        ...options
    };
    
    return await retryWithExponentialBackoff(async () => {
        return await axios(axiosConfig);
    });
}

// Define the ShopifyBulkService class
class ShopifyBulkService {
    constructor(shopDomain, accessToken, logger, storageConnectionString, containerName) {
        this.shopDomain = shopDomain;
        this.accessToken = accessToken;
        this.logger = logger;
        this.storageConnectionString = storageConnectionString;
        this.containerName = containerName;
        this.graphqlEndpoint = `https://${shopDomain}/admin/api/${CONFIG.SHOPIFY_API_VERSION}/graphql.json`;
        
        // Extract store name from domain (remove .myshopify.com)
        this.storeName = shopDomain.replace('.myshopify.com', '');
        
        // Initialize blob service client once
        this.blobServiceClient = BlobServiceClient.fromConnectionString(this.storageConnectionString);
        this.containerClient = this.blobServiceClient.getContainerClient(this.containerName);
        this.containerInitialized = false;
    }

    async ensureContainerExists() {
        if (!this.containerInitialized) {
            try {
                const exists = await this.containerClient.exists();
                if (!exists) {
                    await this.containerClient.create();
                    this.logger.info(`Created blob container: ${this.containerName}`);
                }
                this.containerInitialized = true;
            } catch (error) {
                this.logger.error('Error ensuring container exists:', error.message);
                throw error;
            }
        }
    }

    async makeGraphQLRequest(query, variables = {}) {
        const response = await axiosWithRetry(this.graphqlEndpoint, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'X-Shopify-Access-Token': this.accessToken
            },
            data: {
                query,
                variables
            }
        }, CONFIG.GRAPHQL_TIMEOUT);

        if (response.status !== 200) {
            throw new Error(`HTTP error! status: ${response.status}`);
        }

        const data = response.data;
        
        if (data.errors) {
            throw new Error(`GraphQL errors: ${JSON.stringify(data.errors)}`);
        }

        return data;
    }

    async extractAllProducts() {
        this.logger.info('Starting bulk operation for product extraction');

        const bulkQuery = `
            mutation {
                bulkOperationRunQuery(
                    query: """
                    {
                        products {
                            edges {
                                node {
                                    id
                                    title
                                    handle
                                    description
                                    descriptionHtml
                                    productType
                                    vendor
                                    tags
                                    status
                                    createdAt
                                    updatedAt
                                    publishedAt
                                    templateSuffix
                                    totalInventory
                                    tracksInventory
                                    onlineStoreUrl
                                    onlineStorePreviewUrl
                                    seo {
                                        title
                                        description
                                    }
                                    featuredImage {
                                        id
                                        url
                                        altText
                                        width
                                        height
                                    }
                                    images {
                                        edges {
                                            node {
                                                id
                                                url
                                                altText
                                                width
                                                height
                                            }
                                        }
                                    }
                                    variants {
                                        edges {
                                            node {
                                                id
                                                title
                                                sku
                                                barcode
                                                price
                                                compareAtPrice
                                                taxable
                                                inventoryQuantity
                                                inventoryPolicy
                                                availableForSale
                                                selectedOptions {
                                                    name
                                                    value
                                                }
                                                image {
                                                    id
                                                    url
                                                    altText
                                                }
                                            }
                                        }
                                    }
                                    options {
                                        id
                                        name
                                        position
                                        values
                                    }
                                    collections {
                                        edges {
                                            node {
                                                id
                                                title
                                                handle
                                            }
                                        }
                                    }
                                    metafields {
                                        edges {
                                            node {
                                                id
                                                namespace
                                                key
                                                value
                                                type
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                    """
                ) {
                    bulkOperation {
                        id
                        status
                        errorCode
                        createdAt
                        completedAt
                        objectCount
                        fileSize
                        url
                        partialDataUrl
                    }
                    userErrors {
                        field
                        message
                    }
                }
            }
        `;
        const result = await this.makeGraphQLRequest(bulkQuery);
        
        if (result.data.bulkOperationRunQuery.userErrors.length > 0) {
            throw new Error(`Bulk operation errors: ${JSON.stringify(result.data.bulkOperationRunQuery.userErrors)}`);
        }

        const bulkOperation = result.data.bulkOperationRunQuery.bulkOperation;
        this.logger.info('Bulk operation initiated:', bulkOperation);

        const completedOperation = await this.pollBulkOperationStatus(bulkOperation.id);
        
        if (completedOperation.status === 'COMPLETED') {
            return await this.downloadAndStoreResults(completedOperation);
        } else {
            throw new Error(`Bulk operation failed with status: ${completedOperation.status}, error: ${completedOperation.errorCode}`);
        }
    }

    async pollBulkOperationStatus(operationId, maxAttempts = CONFIG.MAX_POLLING_ATTEMPTS, intervalMs = CONFIG.POLLING_INTERVAL) {
        this.logger.info(`Polling bulk operation status: ${operationId}`);

        const statusQuery = `
            query GetBulkOperation($id: ID!) {
                node(id: $id) {
                    ... on BulkOperation {
                        id
                        status
                        errorCode
                        createdAt
                        completedAt
                        objectCount
                        fileSize
                        url
                        partialDataUrl
                    }
                }
            }
        `;

        for (let attempt = 0; attempt < maxAttempts; attempt++) {
            try {
                const result = await this.makeGraphQLRequest(statusQuery, { id: operationId });
                const operation = result.data.node;
                
                if (!operation) {
                    throw new Error('Bulk operation not found');
                }
                
                this.logger.info(`Bulk operation status (attempt ${attempt + 1}):`, {
                    status: operation.status,
                    objectCount: operation.objectCount,
                    fileSize: operation.fileSize
                });

                if (operation.status === 'COMPLETED' || operation.status === 'FAILED' || operation.status === 'CANCELED') {
                    return operation;
                }

                await new Promise(resolve => setTimeout(resolve, intervalMs));
            } catch (error) {
                this.logger.error(`Error polling bulk operation status (attempt ${attempt + 1}):`, error.message);
                
                // If we're near the end of attempts, throw the error
                if (attempt >= maxAttempts - 3) {
                    throw error;
                }
                
                // Otherwise, wait a bit longer before retrying
                await new Promise(resolve => setTimeout(resolve, intervalMs * 2));
            }
        }

        throw new Error(`Bulk operation polling timeout after ${maxAttempts} attempts`);
    }

    async downloadAndStoreResults(bulkOperation) {
        if (!bulkOperation.url) {
            throw new Error('No download URL available for bulk operation');
        }

        this.logger.info('Downloading bulk operation results from:', bulkOperation.url);

        // Download the JSONL file with retry logic and longer timeout
        const response = await retryWithExponentialBackoff(async () => {
            this.logger.info('Attempting to download bulk operation results...');
            
            const response = await axios({
                method: 'GET',
                url: bulkOperation.url,
                timeout: CONFIG.DOWNLOAD_TIMEOUT,
                headers: {
                    'User-Agent': 'Mozilla/5.0 (compatible; ShopifyBulkService/1.0)'
                }
            });
            
            if (response.status !== 200) {
                throw new Error(`Failed to download results: ${response.status} ${response.statusText}`);
            }
            
            return response;
        }, 5, 2000, 60000);

        this.logger.info('Successfully downloaded bulk operation results, processing data...');
        
        const jsonlData = response.data;
        
        if (!jsonlData || jsonlData.trim().length === 0) {
            throw new Error('Downloaded file is empty or invalid');
        }
        
        // Count products in the JSONL data
        const productCount = this.countProductsInJSONL(jsonlData);
        
        if (productCount === 0) {
            this.logger.warn('Warning: No products found in the downloaded data');
        }
        
        // Generate filename with format: store_name_products_count_date(mm-dd-yy).jsonl
        const now = new Date();
        const dateString = `${String(now.getMonth() + 1).padStart(2, '0')}-${String(now.getDate()).padStart(2, '0')}-${String(now.getFullYear()).slice(-2)}`;
        const filename = `${this.storeName}_products_${productCount}_${dateString}.jsonl`;
        
        // Store in blob storage with retry logic
        const blobUrl = await this.storeInBlobStorage(jsonlData, filename, productCount);

        this.logger.info(`Successfully stored ${productCount} products in blob storage: ${blobUrl}`);

        return {
            success: true,
            bulkOperationId: bulkOperation.id,
            storeName: this.storeName,
            totalProducts: productCount,
            fileSize: bulkOperation.fileSize,
            objectCount: bulkOperation.objectCount,
            completedAt: bulkOperation.completedAt,
            filename: filename,
            blobUrl: blobUrl
        };
    }

    async storeInBlobStorage(jsonlData, filename, productCount) {
        return await retryWithExponentialBackoff(async () => {
            try {
                this.logger.info(`Attempting to store file in blob storage: ${filename}`);
                
                // Ensure container exists
                await this.ensureContainerExists();
                
                // Create directory structure: store_name/products/filename
                const blobPath = `${this.storeName}/products/${filename}`;
                
                // Upload the JSONL file (blob storage will automatically create the folder structure)
                const blockBlobClient = this.containerClient.getBlockBlobClient(blobPath);
                
                const uploadOptions = {
                    blobHTTPHeaders: {
                        blobContentType: 'application/x-ndjson'
                    },
                    metadata: {
                        storeName: this.storeName,
                        dataType: 'products',
                        uploadDate: new Date().toISOString(),
                        productCount: productCount.toString(),
                        originalFileSize: Buffer.byteLength(jsonlData, 'utf8').toString()
                    }
                };

                await blockBlobClient.upload(jsonlData, Buffer.byteLength(jsonlData, 'utf8'), uploadOptions);
                
                this.logger.info(`File uploaded successfully to: ${blobPath}`);
                
                return blockBlobClient.url;

            } catch (error) {
                this.logger.error('Error storing in blob storage:', error);
                throw new Error(`Failed to store in blob storage: ${error.message}`);
            }
        }, 3, 1000, 10000);
    }

    countProductsInJSONL(jsonlData) {
        const lines = jsonlData.trim().split('\n');
        let productCount = 0;

        for (const line of lines) {
            if (!line.trim()) continue;

            try {
                const item = JSON.parse(line);
                // Count lines that contain product IDs
                if (item.id && item.id.includes('Product/')) {
                    productCount++;
                }
            } catch (error) {
                // Skip invalid JSON lines
                this.logger.warn('Skipping invalid JSON line in JSONL data');
            }
        }

        return productCount;
    }

    async processBulkOperationResult(operationId) {
        this.logger.info('Processing completed bulk operation:', operationId);

        const statusQuery = `
            query GetBulkOperation($id: ID!) {
                node(id: $id) {
                    ... on BulkOperation {
                        id
                        status
                        errorCode
                        createdAt
                        completedAt
                        objectCount
                        fileSize
                        url
                        partialDataUrl
                    }
                }
            }
        `;

        const result = await this.makeGraphQLRequest(statusQuery, { id: operationId });
        const operation = result.data.node;

        if (!operation) {
            throw new Error('Bulk operation not found');
        }

        if (operation.status === 'COMPLETED') {
            return await this.downloadAndStoreResults(operation);
        } else {
            throw new Error(`Bulk operation not completed. Status: ${operation.status}, Error: ${operation.errorCode}`);
        }
    }
}

// V4 HTTP Function Registration
app.http('shopifyBulkProducts', {
    methods: ['GET', 'POST'],
    authLevel: 'function',
    handler: async (request, context) => {
        try {
            context.info('Shopify Bulk Products function started');
            
            // Load configuration (now with Key Vault support)
            const config = await loadConfig();
            
            // Get configuration from loaded config
            const shopDomain = config.SHOPIFY_STORE_URL;
            const accessToken = config.SHOPIFY_ACCESS_TOKEN;
            const storageConnectionString = config.AZURE_STORAGE_CONNECTION_STRING;
            const containerName = config.STORAGE_CONTAINER_NAME || CONFIG.DEFAULT_CONTAINER_NAME;
            
            // Debug logging to see what variables are available
            context.info('Configuration check:');
            context.info('shopDomain:', shopDomain ? 'Found' : 'Missing');
            context.info('accessToken:', accessToken ? 'Found' : 'Missing');
            context.info('storageConnectionString:', storageConnectionString ? 'Found' : 'Missing');
            context.info('containerName:', containerName);
            
            if (!shopDomain || !accessToken || !storageConnectionString) {
                const missingVars = [];
                if (!shopDomain) missingVars.push('SHOPIFY_STORE_URL');
                if (!accessToken) missingVars.push('SHOPIFY_ACCESS_TOKEN');
                if (!storageConnectionString) missingVars.push('AZURE_STORAGE_CONNECTION_STRING');
                
                return {
                    status: 400,
                    jsonBody: {
                        error: 'Missing required configuration variables',
                        missingVariables: missingVars,
                        message: 'Please set these variables in Key Vault, environment variables, or local.settings.json'
                    }
                };
            }

            const shopifyService = new ShopifyBulkService(shopDomain, accessToken, context, storageConnectionString, containerName);
            
            // Parse request body once and handle both text and JSON
            let body = null;
            let requestData = null;
            
            try {
                body = await request.text();
                if (body && body.trim()) {
                    try {
                        requestData = JSON.parse(body);
                    } catch (jsonError) {
                        // Body exists but isn't JSON - that's okay for some requests
                        context.info('Request body is not JSON:', jsonError.message);
                    }
                }
            } catch (bodyError) {
                context.warn('Could not read request body:', bodyError.message);
            }
            
            // Get headers (case-insensitive lookup for Shopify headers)
            const headers = {};
            for (const [key, value] of request.headers.entries()) {
                headers[key.toLowerCase()] = value;
            }
            
            // Check if this is a webhook call (bulk operation completed)
            if (body && headers['x-shopify-topic'] === 'bulk_operations/finish') {
                if (!requestData) {
                    return {
                        status: 400,
                        jsonBody: {
                            error: 'Invalid webhook payload',
                            message: 'Webhook body must be valid JSON'
                        }
                    };
                }
                
                context.info('Received bulk operation completion webhook:', requestData);
                
                if (!requestData.admin_graphql_api_id) {
                    return {
                        status: 400,
                        jsonBody: {
                            error: 'Missing admin_graphql_api_id in webhook payload'
                        }
                    };
                }
                
                const result = await shopifyService.processBulkOperationResult(requestData.admin_graphql_api_id);
                
                return {
                    status: 200,
                    jsonBody: result
                };
            }
            
            // Get action parameter from query string or request body
            const url = new URL(request.url);
            const action = url.searchParams.get('action') || (requestData && requestData.action);
            
            // Check for export-products-start action
            if (action === 'export-products-start') {
                context.info('Starting product export based on action parameter');
                const result = await shopifyService.extractAllProducts();
                
                return {
                    status: 200,
                    headers: {
                        'Content-Type': 'application/json'
                    },
                    jsonBody: result
                };
            }
            
            // Health check endpoint
            if (action === 'health' || request.url.includes('/health')) {
                return {
                    status: 200,
                    jsonBody: {
                        status: 'healthy',
                        timestamp: new Date().toISOString(),
                        config: {
                            shopDomain: shopDomain ? 'configured' : 'missing',
                            accessToken: accessToken ? 'configured' : 'missing',
                            storageConnectionString: storageConnectionString ? 'configured' : 'missing',
                            containerName: containerName
                        }
                    }
                };
            }
            
            // Default response if no valid action is provided
            return {
                status: 400,
                jsonBody: {
                    error: 'Invalid or missing action parameter',
                    message: 'Please provide action=export-products-start as query parameter or in request body',
                    availableActions: ['export-products-start', 'health'],
                    requestedAction: action || 'none'
                }
            };
            
        } catch (error) {
            context.error('Error in Shopify bulk products function:', error);
            return {
                status: 500,
                jsonBody: {
                    error: 'Internal server error',
                    message: error.message,
                    timestamp: new Date().toISOString(),
                    // Only include stack trace in development
                    ...(process.env.NODE_ENV === 'development' && { details: error.stack })
                }
            };
        }
    }
});
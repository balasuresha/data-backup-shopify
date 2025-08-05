const { app } = require('@azure/functions');
const axios = require('axios');
const { BlobServiceClient } = require('@azure/storage-blob');
const { loadConfig } = require('../../config-loader');

// Configuration constants
const CONFIG = {
    SHOPIFY_API_VERSION: '2025-07',
    GRAPHQL_TIMEOUT: parseInt(process.env.GRAPHQL_TIMEOUT) || 30000,
    DOWNLOAD_TIMEOUT: parseInt(process.env.DOWNLOAD_TIMEOUT) || 120000,
    MAX_POLLING_ATTEMPTS: parseInt(process.env.MAX_POLLING_ATTEMPTS) || 30,
    POLLING_INTERVAL: parseInt(process.env.POLLING_INTERVAL) || 10000,
};

// Global configuration variable
let config = null;

// Initialize configuration using the centralized loader
async function initializeConfiguration() {
    if (!config) {
        try {
            console.log("Initializing configuration...");
            config = await loadConfig();
            console.log("Configuration initialized successfully");
        } catch (error) {
            console.error("Failed to initialize configuration:", error.message);
            throw error;
        }
    }
    return config;
}

// Utility function for exponential backoff retry
async function retryWithExponentialBackoff(operation, maxRetries = 5, baseDelay = 1000, maxDelay = 30000, backoffMultiplier = 2) {
    let lastError;
    for (let attempt = 0; attempt < maxRetries; attempt++) {
        try {
            return await operation();
        } catch (error) {
            lastError = error;
            if (error.code === 'ENOTFOUND' || error.status === 404 || error.status === 401 || error.status === 403) {
                throw error;
            }
            if (attempt === maxRetries - 1) {
                throw error;
            }
            const exponentialDelay = Math.min(baseDelay * Math.pow(backoffMultiplier, attempt), maxDelay);
            const jitter = Math.random() * 0.1 * exponentialDelay;
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
        this.storeName = shopDomain.replace('.myshopify.com', '');
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
                if (attempt >= maxAttempts - 3) {
                    throw error;
                }
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

        const productCount = this.countProductsInJSONL(jsonlData);
        if (productCount === 0) {
            this.logger.warn('Warning: No products found in the downloaded data');
        }

        const now = new Date();
        const dateString = `${String(now.getMonth() + 1).padStart(2, '0')}-${String(now.getDate()).padStart(2, '0')}-${String(now.getFullYear()).slice(-2)}`;
        const filename = `${this.storeName}_products_${productCount}_${dateString}.jsonl`;
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
                await this.ensureContainerExists();
                const blobPath = `${this.storeName}/products/${filename}`;
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
                if (item.id && item.id.includes('Product/')) {
                    productCount++;
                }
            } catch (error) {
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
app.http('exportBulkProducts', {
    methods: ['GET', 'POST'],
    authLevel: 'function',
    handler: async (request, context) => {
        try {
            context.info('Shopify Bulk Products function started');
            
            // Initialize configuration using centralized loader
            if (!config) {
                await initializeConfiguration();
            }
            
            // Get configuration from loaded config
            const shopDomain = config.SHOPIFY_STORE_URL;
            const accessToken = config.SHOPIFY_ACCESS_TOKEN;
            const storageConnectionString = config.AZURE_STORAGE_CONNECTION_STRING;
            const containerName = config.STORAGE_CONTAINER_NAME;
            
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
                if (!body || !body.trim()) {
                    return {
                        status: 400,
                        jsonBody: {
                            error: 'Missing request body',
                            message: 'Request body is required and must be valid JSON containing the "action": "export-products-start" parameter'
                        }
                    };
                }
                
                try {
                    requestData = JSON.parse(body);
                } catch (jsonError) {
                    return {
                        status: 400,
                        jsonBody: {
                            error: 'Invalid request body',
                            message: 'Request body must be valid JSON',
                            details: jsonError.message
                        }
                    };
                }
            } catch (bodyError) {
                context.warn('Could not read request body:', bodyError.message);
                return {
                    status: 400,
                    jsonBody: {
                        error: 'Failed to read request body',
                        message: bodyError.message
                    }
                };
            }
            
            // Get headers (case-insensitive lookup for Shopify headers)
            const headers = {};
            for (const [key, value] of request.headers.entries()) {
                headers[key.toLowerCase()] = value;
            }
            
            // Check if this is a webhook call (bulk operation completed)
            if (headers['x-shopify-topic'] === 'bulk_operations/finish') {
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
            
            // Check for action in request body
            const action = requestData && requestData.action;
            
            // Check for export-products-start action
            if (action === 'export-products-start') {
                context.info('Starting product export based on action parameter in request body');
                const result = await shopifyService.extractAllProducts();
                
                return {
                    status: 200,
                    headers: {
                        'Content-Type': 'application/json'
                    },
                    jsonBody: result
                };
            }
            
            // Health check endpoint (still allows GET or POST with query parameter)
            const url = new URL(request.url);
            if (url.searchParams.get('action') === 'health' || request.url.includes('/health')) {
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
            
            // Default response if no valid action is provided in the body
            return {
                status: 400,
                jsonBody: {
                    error: 'Invalid or missing action parameter',
                    message: 'The "action" parameter must be provided in the request body as JSON with value "export-products-start"',
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
                    ...(process.env.NODE_ENV === 'development' && { details: error.stack })
                }
            };
        }
    }
});
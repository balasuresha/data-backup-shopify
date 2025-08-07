const { app } = require("@azure/functions");
const { TableClient, AzureNamedKeyCredential } = require('@azure/data-tables');
const { BlobServiceClient } = require('@azure/storage-blob');
const axios = require('axios');
const { v4: uuidv4 } = require("uuid");
const winston = require("winston");
const { createAzureFunctionLogger } = require("../../logging");
const { loadConfig } = require("../../config-loader");

// =================== CONFIGURATION CONSTANTS ===================
// These will be loaded from Azure Key Vault or local.settings.json
let FUNCTION_TIMEOUT_LIMIT = 1740000; // 29 minutes - will be overridden by config
let MAX_RETRY_ATTEMPTS = 5;
let RETRY_DELAY_MS = 2000; // Base retry delay
let MAX_FALLBACK_TIME = 60000; // Maximum fallback time in milliseconds (60 sec)
let POLL_INTERVAL = 30000; // 30 seconds between polls
let MAX_POLL_ATTEMPTS = 60; // Maximum 30 minutes
let DOWNLOAD_TIMEOUT = 600000; // 10 minutes
let SHOPIFY_API_VERSION = '2025-07';
let EXTRACTION_LOG_TABLE_NAME = null;
let EXTRACTION_DETAILS_TABLE_NAME = null;

// =================== ERROR CODES ===================
const RETRYABLE_ERROR_CODES = [
    'ECONNRESET', 'ETIMEDOUT', 'ECONNABORTED', 'ESOCKETTIMEDOUT', 
    'EPIPE', 'ENOTFOUND', 'ENETUNREACH', 'EAI_AGAIN'
];

const RETRYABLE_HTTP_CODES = [408, 429, 500, 502, 503, 504];

// =================== CONFIGURATION VARIABLES ===================
let config = null;
let logger = null;

// =================== CONFIGURATION INITIALIZATION ===================
async function initializeConfiguration() {
    if (!config) {
        try {
            console.log("Initializing configuration from Azure Key Vault or local settings...");
            config = await loadConfig();
            EXTRACTION_LOG_TABLE_NAME = config.STORE_EXTRACTION_LOG_TABLE || 'StoreExtractionLog';
            EXTRACTION_DETAILS_TABLE_NAME = config.STORE_EXTRACTION_DETAILS_TABLE || 'StoreExtractionDataDetails';

            // Load all configurable constants from Azure Key Vault or local settings
            FUNCTION_TIMEOUT_LIMIT = parseInt(config.FUNCTION_TIMEOUT_LIMIT) || 1740000; // 29 minutes
            MAX_RETRY_ATTEMPTS = parseInt(config.MAX_RETRY_ATTEMPTS) || 5;
            RETRY_DELAY_MS = parseInt(config.RETRY_DELAY_MS) || 2000;
            MAX_FALLBACK_TIME = parseInt(config.MAX_FALLBACK_TIME) || 60000;
            POLL_INTERVAL = parseInt(config.POLL_INTERVAL) || 30000;
            MAX_POLL_ATTEMPTS = parseInt(config.MAX_POLL_ATTEMPTS) || 60;
            DOWNLOAD_TIMEOUT = parseInt(config.DOWNLOAD_TIMEOUT) || 600000;
            SHOPIFY_API_VERSION = config.SHOPIFY_API_VERSION || '2024-01';
            
            console.log("Configuration initialized successfully", {
                functionTimeoutLimit: FUNCTION_TIMEOUT_LIMIT,
                maxRetryAttempts: MAX_RETRY_ATTEMPTS,
                retryDelayMs: RETRY_DELAY_MS,
                maxFallbackTime: MAX_FALLBACK_TIME,
                pollInterval: POLL_INTERVAL,
                maxPollAttempts: MAX_POLL_ATTEMPTS,
                downloadTimeout: DOWNLOAD_TIMEOUT,
                shopifyApiVersion: SHOPIFY_API_VERSION
            });
        } catch (error) {
            console.error("Failed to initialize configuration:", error.message);
            throw error;
        }
    }
    return config;
}

//=================== AZURE FUNCTION HANDLERS ===================
app.timer("shopifyBulkOrderExtraction", {
    schedule: "0 0 */6 * * *", // Every 6 hours
    handler: async (myTimer, context) => {
        return await shopifyBulkOrderExtractionHandler(context, myTimer);
    },
});

app.http("shopifyBulkOrderExtractionHttp", {
    methods: ["POST", "GET"],
    authLevel: "function",
    route: "exportBulkOrders",
    handler: async (request, context) => {
        return await shopifyBulkOrderExtractionHandler(context, request);
    },
});

// =================== MAIN HANDLER ===================
async function shopifyBulkOrderExtractionHandler(context, triggerInput) {
    const executionId = uuidv4();
    const functionStartTime = Date.now();
    const startTime = new Date();
    const isHttpTrigger = context.req !== undefined;

    // Initialize configuration first
    if (!config) {
        await initializeConfiguration();
    }

    // Initialize enhanced logging
    logger = createAzureFunctionLogger(context, config);

    logger.azureInfo('Shopify Bulk Order Extraction function started with enhanced logging', {
        executionId,
        triggerType: isHttpTrigger ? 'http' : 'timer',
        timestamp: startTime.toISOString(),
        logFile: logger.logFilePath,
        logsDirectory: logger.logsDir
    });

   let response = {
        status: 200,
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({  
            executionId,
            success: true,
            error: "Completed successfully",
            timestamp: startTime.toISOString(),
            logFile: logger.logFilePath,
        }, null, 2)
    };

    let extractionLogId = null;
    const configData = await validateConfiguration();
    const { tableClient, logTableClient } = await initializeAzureClients(configData, context);
    try {
        await withRetry(
            async () => await tableClient.createTable(),
            { maxRetries: 3, initialDelayMs: 1000, context: context, operationName: 'createTable' }
        );
        
        await withRetry(
            async () => await logTableClient.createTable(),
            { maxRetries: 3, initialDelayMs: 1000, context: context, operationName: 'createLogTable' }
        );
        
        const extractionRecords = await getPendingExtractionRecords(tableClient, context);
        logger.info(`Found ${extractionRecords.length} records to process`);

        const filters = extractFiltersFromRequest(isHttpTrigger ? context.req : null);
        let recordsToProcess = applyFilters(extractionRecords, filters, context);

        const results = await processExtractionRecords(
            recordsToProcess, 
            configData, 
            tableClient, 
            context, 
            functionStartTime
        );

        const endTime = new Date();
        const executionTime = endTime.getTime() - startTime.getTime();

        const summary = {
            totalRecords: extractionRecords.length,
            filtered: recordsToProcess.length,
            processed: results.processedCount,
            skipped: results.skippedCount,
            failed: results.failedCount,
            executionTimeMs: executionTime,
            executionTimeMinutes: Math.round(executionTime / 60000 * 100) / 100
        };

        // Determine overall status based on results
        let overallStatus = 'completed';
        let logErrorMessage = '';

        // Check if all records were processed successfully
        if (results.failedCount > 0) {
            if (results.processedCount === 0) {
                overallStatus = 'failed';
                logErrorMessage = `All ${results.failedCount} records failed to process`;
            } else {
                overallStatus = 'completed_with_errors';
                logErrorMessage = `${results.failedCount} out of ${recordsToProcess.length} records failed`;
            }
        } else if (results.processedCount === recordsToProcess.length) {
            // All records processed successfully, mark as completed
            overallStatus = 'completed';
            logErrorMessage = '';
        } else {
            // Some records were skipped due to timeout or other reasons
            overallStatus = 'completed_with_skips';
            logErrorMessage = `${results.skippedCount} out of ${recordsToProcess.length} records were skipped`;
        }

        const successResponse = {
            executionId,
            extractionLogId,
            success: true,
            message: "Shopify Bulk Order Extraction completed",
            summary,
            timestamp: endTime.toISOString(),
            results: results.detailedResults,
            logFile: logger.logFilePath
        };

        if (isHttpTrigger) {
            response = {
                status: 200,
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify(successResponse, null, 2)
            };
        }

        logger.azureInfo('Shopify Bulk Order Extraction function completed successfully', {
            executionId,
            summary: successResponse.summary,
            logFile: logger.logFilePath
        });

        return response;

    } catch (error) {
        logger.error('Function execution failed:', {
            executionId,
            error: error.message,
            stack: error.stack
        });

        const errorResponse = {
            executionId,
            success: false,
            error: "Function execution failed",
            message: error.message,
            timestamp: new Date().toISOString(),
            stackTrace: process.env.NODE_ENV === 'development' ? error.stack : undefined,
            logFile: logger.logFilePath
        };

        if (isHttpTrigger) {
            response = {
                status: 500,
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify(errorResponse, null, 2)
            };
        }

        return response;
    }
}

// =================== CONFIGURATION VALIDATION ===================
async function validateConfiguration() {
    logger.info("Validating configuration from Azure Key Vault or local settings");
    
    const configData = {
        shopifyStoreUrl: config.SHOPIFY_STORE_URL,
        shopifyAccessToken: config.SHOPIFY_ACCESS_TOKEN,
        azureStorageConnectionString: config.AZURE_STORAGE_CONNECTION_STRING,
        blobContainerName: config.BLOB_CONTAINER_NAME,
        tableStorageAccountName: config.AZURE_STORAGE_ACCOUNT_NAME,
        tableStorageAccountKey: config.AZURE_STORAGE_ACCOUNT_KEY,
        tableName: EXTRACTION_DETAILS_TABLE_NAME,
        logTableName: EXTRACTION_LOG_TABLE_NAME
    };

    const requiredFields = [
        'shopifyStoreUrl', 'shopifyAccessToken', 'azureStorageConnectionString', 
        'blobContainerName', 'tableStorageAccountName', 'tableStorageAccountKey'
    ];

    const missingFields = requiredFields.filter(field => !configData[field]);
    if (missingFields.length > 0) {
        const errorMessage = `Missing required configuration values: ${missingFields.join(', ')}`;
        logger.error('Configuration validation failed', {
            missingFields,
            availableFields: Object.keys(configData).filter(key => configData[key])
        });
        throw new Error(errorMessage);
    }

    logger.info('Configuration validation successful', {
        configuredFields: Object.keys(configData).filter(key => configData[key]),
        tableName: configData.tableName,
        logTableName: configData.logTableName,
        shopifyStore: configData.shopifyStoreUrl?.substring(0, 20) + '...',
        functionTimeoutLimit: FUNCTION_TIMEOUT_LIMIT,
        maxRetryAttempts: MAX_RETRY_ATTEMPTS,
        shopifyApiVersion: SHOPIFY_API_VERSION,
        configSource: 'azure-keyvault-or-local'
    });

    return configData;
}

// =================== AZURE CLIENT INITIALIZATION ===================
async function initializeAzureClients(configData, context) {
    try {
        logger.info('Initializing Azure clients with Key Vault configuration');
        
        const credential = new AzureNamedKeyCredential(
            configData.tableStorageAccountName, 
            configData.tableStorageAccountKey
        );
        
        const tableClient = new TableClient(
            `https://${configData.tableStorageAccountName}.table.core.windows.net`,
            configData.tableName,
            credential
        );

        const logTableClient = new TableClient(
            `https://${configData.tableStorageAccountName}.table.core.windows.net`,
            configData.logTableName,
            credential
        );

        logger.info('Azure clients initialized successfully', {
            tableStorageAccount: configData.tableStorageAccountName,
            tableName: configData.tableName,
            logTableName: configData.logTableName,
            blobContainer: configData.blobContainerName
        });
        
        return { tableClient, logTableClient };
    } catch (error) {
        logger.error('Failed to initialize Azure clients:', {
            error: error.message,
            stack: error.stack
        });
        throw new Error(`Azure client initialization failed: ${error.message}`);
    }
}

// =================== REQUEST FILTERING ===================
function extractFiltersFromRequest(request) {
    if (!request || !request.query) {
        return {};
    }

    const filters = {
        statusFilter: request.query.status ? request.query.status.split(',') : null,
        storeFilter: request.query.storeName || null
    };

    logger.info('Request filters extracted', filters);
    return filters;
}

function applyFilters(records, filters, context) {
    let filteredRecords = [...records];

    if (filters.statusFilter) {
        filteredRecords = filteredRecords.filter(r => filters.statusFilter.includes(r.status));
        logger.info(`Filtered to ${filteredRecords.length} records with status: ${filters.statusFilter.join(', ')}`);
    }

    if (filters.storeFilter) {
        filteredRecords = filteredRecords.filter(r => r.storeName === filters.storeFilter);
        logger.info(`Filtered to ${filteredRecords.length} records for store: ${filters.storeFilter}`);
    }

    return filteredRecords;
}

// =================== RECORD PROCESSING ===================
async function processExtractionRecords(recordsToProcess, configData, tableClient, context, functionStartTime) {
    let processedCount = 0;
    let skippedCount = 0;
    let failedCount = 0;
    const detailedResults = [];

    logger.info(`Starting processing of ${recordsToProcess.length} extraction records`);

    for (const record of recordsToProcess) {
        const timeElapsed = Date.now() - functionStartTime;
        if (timeElapsed > FUNCTION_TIMEOUT_LIMIT) {
            logger.warn(`Approaching function timeout (${timeElapsed}ms elapsed). Stopping after processing ${processedCount} records.`);
            skippedCount = recordsToProcess.length - processedCount;
            break;
        }
        
        try {
            logger.info(`Processing record ${record.rowKey} (${processedCount + 1}/${recordsToProcess.length})`);
            
            const result = await processExtractionRecord(record, configData, tableClient, context);
            detailedResults.push({
                recordId: record.rowKey,
                status: 'success',
                blobUrl: result.blobUrl,
                fileSizeBytes: result.fileSizeBytes,
                processingTime: result.processingTime
            });
            
            processedCount++;
            
            if (processedCount % 10 === 0 || processedCount === recordsToProcess.length) {
                logger.info(`Processing progress: ${processedCount}/${recordsToProcess.length} records completed`);
            }
            
        } catch (error) {
            logger.error(`Error processing record ${record.rowKey}:`, {
                error: error.message,
                stack: error.stack,
                recordId: record.rowKey
            });
            
            detailedResults.push({
                recordId: record.rowKey,
                status: 'failed',
                error: error.message
            });
            
            failedCount++;
        }
    }

    logger.info('Record processing completed', {
        processedCount,
        skippedCount,
        failedCount,
        totalRecords: recordsToProcess.length
    });

    return {
        processedCount,
        skippedCount,
        failedCount,
        detailedResults
    };
}

// =================== DATABASE OPERATIONS ===================
async function getPendingExtractionRecords(tableClient, context) {
    try {
        logger.info('Fetching pending extraction records from Azure Table Storage');
        
        const records = [];
        const entities = tableClient.listEntities({
            queryOptions: {
                filter: "status eq 'open' or status eq 'inprogress' or status eq 'failed' or status eq 'operation_conflict'"
            }
        });

        for await (const entity of entities) {
            records.push({
                partitionKey: entity.partitionKey,
                rowKey: entity.rowKey,
                extractionStartDate: entity.extractionStartDate,
                extractionEndDate: entity.extractionEndDate,
                status: entity.status,
                errorMessage: entity.errorMessage || '',
                storeName: entity.storeName,
                failCount: parseInt(entity.failCount || '0', 10)
            });
        }

        const statusCounts = records.reduce((acc, record) => {
            acc[record.status] = (acc[record.status] || 0) + 1;
            return acc;
        }, {});
        
        logger.info('Pending extraction records retrieved', {
            totalRecords: records.length,
            statusBreakdown: statusCounts
        });
        
        return records;
    } catch (error) {
        logger.error('Error fetching extraction records:', {
            error: error.message,
            stack: error.stack
        });
        throw error;
    }
}

async function updateRecordStatus(tableClient, partitionKey, rowKey, status, errorMessage = '', failCount = null, blobUrl = '', fileSizeBytes = 0) {
    try {
        const entity = {
            partitionKey,
            rowKey,
            status,
            errorMessage: errorMessage ? errorMessage.substring(0, 255) : '',
            lastUpdated: new Date().toISOString(),
            blobUrl: blobUrl || '',
            fileSizeBytes: fileSizeBytes || 0
        };
        
        if (failCount !== null) {
            entity.failCount = String(failCount);
        }

        await withRetry(
            async () => await tableClient.updateEntity(entity, 'Merge'),
            { maxRetries: 3, initialDelayMs: 1000, operationName: 'updateRecordStatus' }
        );
        
        logger.debug('Record status updated', {
            rowKey,
            status,
            blobUrl: blobUrl ? 'provided' : 'empty',
            fileSizeBytes
        });
        
    } catch (error) {
        logger.error('Error updating record status:', {
            error: error.message,
            rowKey,
            status
        });
        throw error;
    }
}

// =================== MAIN PROCESSING LOGIC ===================
async function processExtractionRecord(record, configData, tableClient, context) {
    const startTime = Date.now();
    const { partitionKey, rowKey, extractionStartDate, extractionEndDate, storeName, failCount = 0 } = record;
    
    logger.info(`Processing extraction record`, {
        recordId: rowKey,
        dateRange: `${extractionStartDate} to ${extractionEndDate}`,
        storeName,
        failCount
    });
    
    if (failCount >= 5) {
        logger.warn(`Record ${rowKey} has failed ${failCount} times, marking as permanent failure`);
        await updateRecordStatus(
            tableClient, 
            partitionKey, 
            rowKey, 
            'permanent_failure', 
            'Exceeded maximum retry attempts'
        );
        return { blobUrl: '', fileSizeBytes: 0, processingTime: Date.now() - startTime };
    }

    await updateRecordStatus(tableClient, partitionKey, rowKey, 'inprogress');
    
    try {
        const shopifyAPI = new ShopifyBulkAPI(configData, context);
        
        // Step 1: Create bulk operation
        logger.info(`Step 1: Creating bulk operation for record ${rowKey}`);
        const bulkOperationId = await createBulkOperationWithRetry(
            shopifyAPI,
            extractionStartDate,
            extractionEndDate,
            context
        );
        
        // Step 2: Poll for completion
        logger.info(`Step 2: Polling bulk operation ${bulkOperationId} for completion`);
        const downloadUrl = await shopifyAPI.pollBulkOperation(bulkOperationId);

        if (downloadUrl === null) {
            logger.info(`No orders found for ${storeName} between ${extractionStartDate} and ${extractionEndDate}`);
            await updateRecordStatus(tableClient, partitionKey, rowKey, 'completed', 'No orders found in date range');
            return { blobUrl: '', fileSizeBytes: 0, processingTime: Date.now() - startTime };
        }
        
        // Step 3: Download raw data and upload directly to blob
        logger.info(`Step 3: Downloading and uploading data for record ${rowKey}`);
        const { blobUrl, fileSizeBytes } = await downloadAndUploadRawData(
            configData,
            downloadUrl,
            rowKey,
            context,
            storeName || partitionKey
        );
        
        // Step 4: Update record as completed
        await updateRecordStatus(tableClient, partitionKey, rowKey, 'completed', '', null, blobUrl, fileSizeBytes);
        
        logger.info(`Successfully processed record ${rowKey}`, {
            blobUrl,
            fileSizeBytes,
            processingTime: Date.now() - startTime
        });
        
        return { blobUrl, fileSizeBytes, processingTime: Date.now() - startTime };
        
    } catch (error) {
        logger.error(`Error in processExtractionRecord for record ${rowKey}:`, {
            error: error.message,
            stack: error.stack,
            recordId: rowKey
        });
        
        const isRetryableError = RETRYABLE_ERROR_CODES.includes(error.code) || 
                                error.message.includes('timeout') ||
                                error.message.includes('network');
        
        const newFailCount = isRetryableError ? failCount + 1 : failCount;
        const newStatus = error.message?.includes("already in progress") ? 'operation_conflict' : 'failed';
        
        await updateRecordStatus(
            tableClient, 
            partitionKey, 
            rowKey, 
            newStatus, 
            error.message,
            newFailCount
        );
        
        throw error;
    }
}

// =================== BULK OPERATIONS ===================
async function createBulkOperationWithRetry(shopifyAPI, startDate, endDate, context) {
    let retryCount = 0;
    const maxRetries = 5;
    
    logger.info('Creating bulk operation with retry capability', {
        startDate,
        endDate,
        maxRetries
    });
    
    while (retryCount <= maxRetries) {
        try {
            return await shopifyAPI.createBulkOperation(startDate, endDate);
        } catch (error) {
            if (error.message?.includes("already in progress")) {
                if (retryCount >= maxRetries) {
                    throw new Error(`Failed to create bulk operation after ${maxRetries + 1} attempts due to ongoing operations`);
                }
                
                // Updated exponential backoff: Math.min(delay * 2^retryCount, maxFallBackTime)
                const waitTime = Math.min(RETRY_DELAY_MS * Math.pow(2, retryCount), MAX_FALLBACK_TIME);
                logger.warn(`Bulk operation in progress, waiting ${waitTime}ms before retry ${retryCount + 1}/${maxRetries + 1}`);
                await delay(waitTime);
                retryCount++;
            } else {
                throw error;
            }
        }
    }
}

// =================== SHOPIFY API CLASS ===================
class ShopifyBulkAPI {
    constructor(configData, context) {
        this.shopDomain = configData.shopifyStoreUrl;
        this.accessToken = configData.shopifyAccessToken;
        this.apiVersion = SHOPIFY_API_VERSION; // Now loaded from config
        this.baseURL = this.shopDomain.startsWith('http') ? 
            `${this.shopDomain}/admin/api/${this.apiVersion}` :
            `https://${this.shopDomain}/admin/api/${this.apiVersion}`;
        this.context = context;
        
        logger.info('ShopifyBulkAPI initialized with configuration values', {
            shopDomain: this.shopDomain,
            apiVersion: this.apiVersion,
            maxRetryAttempts: MAX_RETRY_ATTEMPTS,
            retryDelayMs: RETRY_DELAY_MS,
            maxFallbackTime: MAX_FALLBACK_TIME,
            pollInterval: POLL_INTERVAL,
            maxPollAttempts: MAX_POLL_ATTEMPTS,
            downloadTimeout: DOWNLOAD_TIMEOUT,
            configSource: 'azure-keyvault-or-local'
        });
    }

    async createBulkOperation(startDate, endDate) {
        const endpoint = `${this.baseURL}/graphql.json`;
        const mutation = this.buildBulkOperationMutation(startDate, endDate);

        logger.info('Creating Shopify bulk operation', {
            startDate,
            endDate,
            endpoint: endpoint.substring(0, 50) + '...'
        });

        const response = await withRetry(
            async () => {
                return await axios.post(
                    endpoint,
                    { query: mutation },
                    {
                        headers: {
                            'X-Shopify-Access-Token': this.accessToken,
                            'Content-Type': 'application/json'
                        },
                        timeout: 30000
                    }
                );
            },
            { maxRetries: MAX_RETRY_ATTEMPTS, initialDelayMs: RETRY_DELAY_MS, context: this.context, operationName: 'createBulkOperation' }
        );

        if (response.data.errors) {
            throw new Error(`GraphQL errors: ${JSON.stringify(response.data.errors)}`);
        }

        const userErrors = response.data.data.bulkOperationRunQuery.userErrors;
        if (userErrors && userErrors.length > 0) {
            const alreadyInProgressError = userErrors.find(err => 
                err.message && err.message.includes("already in progress")
            );
            
            if (alreadyInProgressError) {
                throw new Error(alreadyInProgressError.message);
            }
            
            throw new Error(`User errors: ${JSON.stringify(userErrors)}`);
        }

        const bulkOperation = response.data.data.bulkOperationRunQuery.bulkOperation;
        logger.info(`Created bulk operation successfully`, {
            operationId: bulkOperation.id,
            status: bulkOperation.status
        });
        
        return bulkOperation.id;
    }

    buildBulkOperationMutation(startDate, endDate) {
        return `
            mutation {
                bulkOperationRunQuery(
                    query: """
                    {
                        orders(query: "created_at:>='${startDate}' AND created_at:<='${endDate}'") {
                            edges {
                                node {
                                    id
                                    name
                                    email
                                    createdAt
                                    updatedAt
                                    totalPriceSet {
                                        shopMoney {
                                            amount
                                            currencyCode
                                        }
                                    }
                                    subtotalPriceSet {
                                        shopMoney {
                                            amount
                                            currencyCode
                                        }
                                    }
                                    totalTaxSet {
                                        shopMoney {
                                            amount
                                            currencyCode
                                        }
                                    }
                                    totalShippingPriceSet {
                                        shopMoney {
                                            amount
                                            currencyCode
                                        }
                                    }
                                    displayFinancialStatus
                                    displayFulfillmentStatus
                                    processedAt
                                    cancelledAt
                                    cancelReason
                                    tags
                                    note
                                    phone
                                    test
                                    totalDiscountsSet {
                                        shopMoney {
                                            amount
                                            currencyCode
                                        }
                                    }
                                    totalWeight
                                    customer {
                                        id
                                        email
                                        firstName
                                        lastName
                                        phone
                                        createdAt
                                        updatedAt
                                    }
                                    shippingAddress {
                                        firstName
                                        lastName
                                        company
                                        address1
                                        address2
                                        city
                                        province
                                        country
                                        zip
                                        phone
                                    }
                                    billingAddress {
                                        firstName
                                        lastName
                                        company
                                        address1
                                        address2
                                        city
                                        province
                                        country
                                        zip
                                        phone
                                    }
                                    lineItems(first: 50) {
                                        edges {
                                            node {
                                                id
                                                title
                                                quantity
                                                originalUnitPriceSet {
                                                    shopMoney {
                                                        amount
                                                        currencyCode
                                                    }
                                                }
                                                sku
                                                variantTitle
                                                vendor
                                                product {
                                                    id
                                                }
                                                variant {
                                                    id
                                                }
                                                taxable
                                                totalDiscountSet {
                                                    shopMoney {
                                                        amount
                                                        currencyCode
                                                    }
                                                }
                                            }
                                        }
                                    }
                                    discountApplications(first: 5) {
                                        edges {
                                            node {
                                                __typename
                                                value {
                                                    ... on MoneyV2 {
                                                        amount
                                                        currencyCode
                                                    }
                                                    ... on PricingPercentageValue {
                                                        percentage
                                                    }
                                                }
                                                ... on DiscountCodeApplication {
                                                    code
                                                    targetSelection
                                                    targetType
                                                    allocationMethod
                                                }
                                                ... on ManualDiscountApplication {
                                                    description
                                                    targetSelection
                                                    targetType
                                                    allocationMethod
                                                }
                                                ... on AutomaticDiscountApplication {
                                                    targetSelection
                                                    targetType
                                                    allocationMethod
                                                }
                                            }
                                        }
                                    }
                                    fulfillments(first: 3) {
                                        id
                                        status
                                        createdAt
                                        updatedAt
                                        trackingInfo(first: 1) {
                                            company
                                            number
                                            url
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
                    }
                    userErrors {
                        field
                        message
                    }
                }
            }
        `;
    }

    async pollBulkOperation(operationId) {
        const query = `
            query {
                node(id: "${operationId}") {
                    ... on BulkOperation {
                        id
                        status
                        errorCode
                        createdAt
                        completedAt
                        objectCount
                        fileSize
                        url
                        type
                    }
                }
            }
        `;

        let attempts = 0;
        const endpoint = `${this.baseURL}/graphql.json`;

        logger.info('Starting to poll bulk operation', {
            operationId,
            maxAttempts: MAX_POLL_ATTEMPTS,
            pollInterval: POLL_INTERVAL
        });

        while (attempts < MAX_POLL_ATTEMPTS) {
            const response = await withRetry(
                async () => {
                    return await axios.post(
                        endpoint,
                        { query },
                        {
                            headers: {
                                'X-Shopify-Access-Token': this.accessToken,
                                'Content-Type': 'application/json'
                            },
                            timeout: 30000
                        }
                    );
                },
                { maxRetries: 3, initialDelayMs: RETRY_DELAY_MS, context: this.context, operationName: 'pollBulkOperation' }
            );

            if (response.data.errors) {
                throw new Error(`GraphQL errors: ${JSON.stringify(response.data.errors)}`);
            }

            const bulkOperation = response.data.data.node;
            
            if (attempts % 10 === 0 || bulkOperation.status !== 'RUNNING') {
                logger.info(`Bulk operation status check (attempt ${attempts + 1})`, {
                    operationId,
                    status: bulkOperation.status,
                    objectCount: bulkOperation.objectCount || 0,
                    attempt: attempts + 1
                });
            }
            
            if (bulkOperation.status === 'COMPLETED') {
                if (bulkOperation.objectCount === 0 || !bulkOperation.objectCount) {
                    logger.info('Bulk operation completed with zero objects - no data to download');
                    return null;
                }
                
                if (!bulkOperation.url) {
                    logger.warn('Warning: Bulk operation completed but no download URL provided despite having objects');
                    return null;
                }
                
                logger.info(`Bulk operation completed successfully`, {
                    operationId,
                    objectCount: bulkOperation.objectCount,
                    fileSize: bulkOperation.fileSize,
                    downloadUrl: 'available'
                });
                return bulkOperation.url;
            } else if (bulkOperation.status === 'FAILED') {
                throw new Error(`Bulk operation failed with status: ${bulkOperation.status}, error: ${bulkOperation.errorCode || 'unknown'}`);
            } else if (bulkOperation.status === 'CANCELED') {
                throw new Error(`Bulk operation was canceled with status: ${bulkOperation.status}`);
            }

            await delay(POLL_INTERVAL);
            attempts++;
        }

        throw new Error('Bulk operation timed out after waiting 30 minutes');
    }
}

// =================== RAW DATA DOWNLOAD AND UPLOAD ===================
async function downloadAndUploadRawData(configData, downloadUrl, recordId, context, storeName) {
    try {
        logger.info('Starting raw JSONL data download and upload', {
            downloadUrl: downloadUrl.substring(0, 100) + '...',
            recordId,
            storeName
        });
        
        // Initialize Azure Blob Storage client
        const blobServiceClient = BlobServiceClient.fromConnectionString(configData.azureStorageConnectionString);
        const containerClient = blobServiceClient.getContainerClient(configData.blobContainerName);
        
        // Ensure container exists
        await withRetry(
            async () => await containerClient.createIfNotExists(),
            { maxRetries: 3, initialDelayMs: 1000, context: context, operationName: 'createContainer' }
        );

        // Create blob name with timestamp
        const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
        const directoryName = storeName.replace(/[^a-zA-Z0-9_-]/g, '_').toLowerCase();
        const blobName = `${directoryName}/shopify-orders-raw-${recordId}-${timestamp}.jsonl`;
        const blockBlobClient = containerClient.getBlockBlobClient(blobName);

        logger.info('Downloading data from Shopify and uploading to Azure Blob', {
            blobName,
            directoryName,
            containerName: configData.blobContainerName
        });

        // Download from Shopify and upload directly to blob as stream
        const downloadResponse = await withRetry(
            async () => {
                return await axios.get(downloadUrl, {
                    responseType: 'stream',
                    timeout: DOWNLOAD_TIMEOUT,
                    maxContentLength: Infinity,
                    maxBodyLength: Infinity,
                });
            },
            { maxRetries: 3, initialDelayMs: 5000, context: context, operationName: 'downloadShopifyData' }
        );

        // Track file size
        let fileSizeBytes = 0;
        downloadResponse.data.on('data', (chunk) => {
            fileSizeBytes += chunk.length;
        });

        // Upload stream directly to blob
        await withRetry(
            async () => {
                return await blockBlobClient.uploadStream(
                    downloadResponse.data,
                    undefined, 
                    undefined,
                    {
                        blobHTTPHeaders: {
                            blobContentType: 'application/jsonl'
                        },
                        metadata: {
                            recordId: recordId,
                            storeName: directoryName,
                            extractionDate: timestamp,
                            contentType: 'shopify-orders-raw',
                            source: 'shopify-bulk-api',
                            configSource: 'azure-keyvault'
                        },
                        concurrency: 5,
                        bufferSize: 4 * 1024 * 1024,
                        maxBuffers: 20
                    }
                );
            },
            { maxRetries: 3, initialDelayMs: 2000, context: context, operationName: 'uploadToBlob' }
        );
        
        const blobUrl = blockBlobClient.url;
        logger.info(`Successfully uploaded raw JSONL data to blob storage`, {
            blobName,
            blobUrl,
            fileSizeBytes,
            fileSizeKB: Math.round(fileSizeBytes / 1024),
            fileSizeMB: Math.round(fileSizeBytes / (1024 * 1024))
        });
        
        return { blobUrl, fileSizeBytes };
        
    } catch (error) {
        logger.error('Error downloading and uploading raw data:', {
            error: error.message,
            stack: error.stack,
            recordId,
            storeName
        });
        throw error;
    }
}

// =================== UTILITY FUNCTIONS ===================
function delay(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

// =================== UPDATED RETRY MECHANISM ===================
/**
 * Implements exponential backoff retry mechanism with updated formula
 * Formula: Math.min(delay * 2^retryCount, maxFallBackTime)
 * @param {Function} operation - Async function to retry
 * @param {Object} options - Retry configuration options
 * @returns {Promise} - Result of the operation
 */
async function withRetry(operation, options = {}) {
    const {
        maxRetries = MAX_RETRY_ATTEMPTS,
        initialDelayMs = RETRY_DELAY_MS,
        maxFallBackTime = MAX_FALLBACK_TIME,
        context = null,
        operationName = 'unnamed operation'
    } = options;
    
    let retryCount = 0;
    
    while (true) {
        try {
            return await operation();
        } catch (error) {
            const errorCode = error.code || '';
            const errorMessage = error.message || '';
            const statusCode = error.response?.status;
            
            // Determine if error is retryable
            const isRetryable = 
                RETRYABLE_ERROR_CODES.includes(errorCode) || 
                errorMessage.toLowerCase().includes('timeout') ||
                errorMessage.toLowerCase().includes('network') ||
                errorMessage.toLowerCase().includes('econnreset') ||
                (error.isAxiosError && RETRYABLE_HTTP_CODES.includes(statusCode));
                
            // If not retryable or max retries reached, throw the error
            if (!isRetryable || retryCount >= maxRetries) {
                if (logger) {
                    logger.error(`${operationName} failed ${retryCount >= maxRetries ? 'after maximum retries' : 'with non-retryable error'}`, {
                        operation: operationName,
                        error: errorMessage,
                        retryCount,
                        maxRetries,
                        isRetryable
                    });
                }
                throw error;
            }
            
            // Calculate next delay using updated exponential backoff formula
            // Formula: Math.min(delay * 2^retryCount, maxFallBackTime)
            const delay = Math.min(initialDelayMs * Math.pow(2, retryCount), maxFallBackTime);
            
            // Log the retry attempt
            if (logger) {
                const errorDetails = error.isAxiosError ? 
                    `Status: ${statusCode}, URL: ${error.config?.url?.split('?')[0]}` : 
                    errorMessage;
                    
                logger.warn(`${operationName} - Retry attempt`, {
                    operation: operationName,
                    retryCount: retryCount + 1,
                    maxRetries: maxRetries + 1,
                    delayMs: delay,
                    errorDetails,
                    isAxiosError: error.isAxiosError
                });
            }
            
            // Wait before retrying
            await new Promise(resolve => setTimeout(resolve, delay));
            retryCount++;
        }
    }
}

module.exports = shopifyBulkOrderExtractionHandler;
const { app } = require("@azure/functions");
const winston = require("winston");
const { v4: uuidv4 } = require("uuid");
const { createAzureFunctionLogger } = require("../../logging");
const { TableClient, AzureNamedKeyCredential } = require('@azure/data-tables');

const { loadConfig } = require("../../config-loader");

// Configuration state
let config = null;
let logger = null;
let isConfigurationLoaded = false;

// Configuration variables
let STORAGE_ACCOUNT_NAME = null;
let STORAGE_ACCOUNT_KEY = null;
let AZURE_STORAGE_CONNECTION_STRING = null;
let STORE_EXTRACTION_LOG_TABLE = null; // source table for extraction logs
let STORE_EXTRACTION_DETAILS_TABLE = null; // table for extraction date by store

// Table clients
let sourceTableClient = null;
let targetTableClient = null;

// Constants
const VALID_FREQUENCIES = ['full', 'yearly', 'quarterly', 'monthly'];
const MAX_RETRY_ATTEMPTS = 3;
const RETRY_DELAY_MS = 1000;
const MAX_ENTITIES_PER_BATCH = 100;

// Utility function for retry logic
async function retryOperation(operation, maxAttempts = MAX_RETRY_ATTEMPTS, delayMs = RETRY_DELAY_MS) {
    let lastError;
    
    for (let attempt = 1; attempt <= maxAttempts; attempt++) {
        try {
            return await operation();
        } catch (error) {
            lastError = error;
            
            // Don't retry on certain error types
            if (error.statusCode === 400 || error.statusCode === 401 || error.statusCode === 403) {
                throw error;
            }
            
            if (attempt < maxAttempts) {
                console.warn(`Operation failed (attempt ${attempt}/${maxAttempts}), retrying in ${delayMs}ms:`, error.message);
                await new Promise(resolve => setTimeout(resolve, delayMs));
                delayMs *= 2; // Exponential backoff
            }
        }
    }
    
    throw new Error(`Operation failed after ${maxAttempts} attempts. Last error: ${lastError.message}`);
}

// Input validation and sanitization
function validateAndSanitizeInput(requestBody) {
    const errors = [];
    const sanitized = {};
    
    // Validate storeName
    if (requestBody.initiateDateExtraction !== undefined) {
        if (typeof requestBody.initiateDateExtraction !== 'boolean') {
            if (requestBody.initiateDateExtraction === 'true' || requestBody.initiateDateExtraction === true) {
                sanitized.initiateDateExtraction = true;
            } else if (requestBody.initiateDateExtraction === 'false' || requestBody.initiateDateExtraction === false) {
                sanitized.initiateDateExtraction = false;
            } else {
                errors.push('initiateDateExtraction must be a boolean (true/false)');
            }
        } else {
            sanitized.initiateDateExtraction = requestBody.initiateDateExtraction;
        }
    } else {
        errors.push('initiateDateExtraction parameter is required and must be set to true to proceed with extraction');
    }
    
    // Validate health check
    if (requestBody.health !== undefined) {
        sanitized.health = requestBody.health === 'true' || requestBody.health === true;
    }
    
    return { errors, sanitized };
}

// Enhanced configuration loader with better error handling
async function loadConfigurationValues() {
    console.log("Loading configuration values...");
    
    try {
        // Use the existing config loader which handles both Key Vault and local.settings.json
        const loadedConfig = await loadConfig();
        
        // Map the loaded configuration to the variables we need
        STORAGE_ACCOUNT_NAME = loadedConfig.AZURE_STORAGE_ACCOUNT_NAME;
        STORAGE_ACCOUNT_KEY = loadedConfig.AZURE_STORAGE_ACCOUNT_KEY;
        AZURE_STORAGE_CONNECTION_STRING = loadedConfig.AZURE_STORAGE_CONNECTION_STRING;
        STORE_EXTRACTION_LOG_TABLE = loadedConfig.STORE_EXTRACTION_LOG_TABLE;
        STORE_EXTRACTION_DETAILS_TABLE = loadedConfig.STORE_EXTRACTION_DETAILS_TABLE;
        
        console.log("Configuration mapping completed:", {
            hasStorageAccountName: !!STORAGE_ACCOUNT_NAME,
            hasStorageAccountKey: !!STORAGE_ACCOUNT_KEY,
            hasConnectionString: !!AZURE_STORAGE_CONNECTION_STRING,
            sourceTableName: STORE_EXTRACTION_LOG_TABLE,
            targetTableName: STORE_EXTRACTION_DETAILS_TABLE
        });
        
        // Validate required configuration
        const missingConfig = [];
        
        // Check Azure Storage credentials
        if (!AZURE_STORAGE_CONNECTION_STRING && (!STORAGE_ACCOUNT_NAME || !STORAGE_ACCOUNT_KEY)) {
            missingConfig.push('Azure Storage credentials (connection string or account name/key)');
        }
        
        // Check table names
        if (!STORE_EXTRACTION_LOG_TABLE) {
            missingConfig.push('STORE_EXTRACTION_LOG_TABLE');
        }
        if (!STORE_EXTRACTION_DETAILS_TABLE) {
            missingConfig.push('STORE_EXTRACTION_DETAILS_TABLE');
        }
        
        if (missingConfig.length > 0) {
            throw new Error(`Missing required configuration: ${missingConfig.join(', ')}`);
        }
        
        // Validate connection string format if provided
        if (AZURE_STORAGE_CONNECTION_STRING && !validateConnectionString(AZURE_STORAGE_CONNECTION_STRING)) {
            throw new Error('Azure Storage connection string format is invalid');
        }
        
        console.log("Configuration validation successful");
        isConfigurationLoaded = true;
        
    } catch (error) {
        console.error("Configuration loading failed:", error.message);
        throw new Error(`Configuration loading failed: ${error.message}`);
    }
}

function validateConnectionString(connectionString) {
    if (!connectionString || typeof connectionString !== 'string') {
        return false;
    }
    
    // Check for required components
    const requiredComponents = [
        'DefaultEndpointsProtocol=',
        'AccountName=',
        'AccountKey=',
        'EndpointSuffix='
    ];
    
    const hasAllComponents = requiredComponents.every(component => 
        connectionString.includes(component)
    );
    
    if (!hasAllComponents) {
        console.error('Connection string missing required components:', {
            connectionStringPrefix: connectionString.substring(0, 50) + '...',
            missingComponents: requiredComponents.filter(comp => !connectionString.includes(comp))
        });
        return false;
    }
    
    return true;
}

async function initializeConfiguration() {
    if (!config || !isConfigurationLoaded) {
        try {
            console.log("Initializing configuration...");
            
            // Load configuration values (this also loads the main config)
            await loadConfigurationValues();
            
            // Set the config object (it's used by the logger)
            config = await loadConfig();
            
            console.log("Configuration initialized successfully");
        } catch (error) {
            console.error("Failed to initialize configuration:", error.message);
            throw new Error(`Configuration initialization failed: ${error.message}`);
        }
    }
    return config;
}

function initializeTableClients() {
    if (!isConfigurationLoaded) {
        throw new Error('Configuration must be loaded before initializing table clients');
    }
    
    console.log('=== INITIALIZING TABLE CLIENTS (FIXED VERSION) ===');
    
    // Validate table names
    if (!STORE_EXTRACTION_LOG_TABLE || !STORE_EXTRACTION_DETAILS_TABLE) {
        throw new Error('Table names are not configured properly');
    }
    
    console.log('Table names validated:', {
        sourceTable: STORE_EXTRACTION_LOG_TABLE,
        targetTable: STORE_EXTRACTION_DETAILS_TABLE
    });

    if (AZURE_STORAGE_CONNECTION_STRING) {
        try {
            console.log('Parsing connection string manually...');
            
            // Parse connection string into components
            const connectionParams = {};
            const parts = AZURE_STORAGE_CONNECTION_STRING.split(';');
            
            parts.forEach(part => {
                if (part.includes('=')) {
                    const [key, ...valueParts] = part.split('=');
                    connectionParams[key.trim()] = valueParts.join('=').trim();
                }
            });
            
            console.log('Connection string parsed:', {
                hasProtocol: !!connectionParams.DefaultEndpointsProtocol,
                hasAccountName: !!connectionParams.AccountName,
                hasAccountKey: !!connectionParams.AccountKey,
                hasEndpointSuffix: !!connectionParams.EndpointSuffix,
                accountName: connectionParams.AccountName
            });
            
            // Extract required values
            const accountName = connectionParams.AccountName;
            const accountKey = connectionParams.AccountKey;
            const endpointSuffix = connectionParams.EndpointSuffix || 'core.windows.net';
            
            if (!accountName || !accountKey) {
                throw new Error('Missing AccountName or AccountKey in connection string');
            }
            
            // Create credential and service URL
            const credential = new AzureNamedKeyCredential(accountName, accountKey);
            const serviceUrl = `https://${accountName}.table.${endpointSuffix}`;
            
            console.log('Creating table clients with explicit credentials:', {
                serviceUrl: serviceUrl,
                accountName: accountName,
                sourceTable: STORE_EXTRACTION_LOG_TABLE,
                targetTable: STORE_EXTRACTION_DETAILS_TABLE
            });
            
            // Create table clients using explicit credentials (this works!)
            sourceTableClient = new TableClient(serviceUrl, STORE_EXTRACTION_LOG_TABLE, credential);
            targetTableClient = new TableClient(serviceUrl, STORE_EXTRACTION_DETAILS_TABLE, credential);
            
            console.log('Table clients created successfully using parsed connection string');
            return;
            
        } catch (parseError) {
            console.error('Failed to parse connection string:', parseError.message);
            throw new Error(`Connection string parsing failed: ${parseError.message}`);
        }
    }
}

// Enhanced date validation and parsing
function parseAndValidateDate(dateString, fieldName) {
    if (!dateString) {
        throw new Error(`${fieldName} is required`);
    }
    
    const date = new Date(dateString);
    if (isNaN(date.getTime())) {
        throw new Error(`Invalid ${fieldName} format: ${dateString}. Expected format: YYYY-MM-DD`);
    }
    
    // Additional validation for reasonable date ranges
    const minDate = new Date('1900-01-01');
    const maxDate = new Date('2100-12-31');
    
    if (date < minDate || date > maxDate) {
        throw new Error(`${fieldName} must be between 1900-01-01 and 2100-12-31`);
    }
    
    return date;
}

// Enhanced date range splitting with better error handling
function splitDateRangeByFrequency(startDateStr, endDateStr, frequency = 'monthly') {
    const dateRanges = [];
    
    // Validate and parse dates
    const startDate = parseAndValidateDate(startDateStr, 'startDate');
    const endDate = parseAndValidateDate(endDateStr, 'endDate');
    
    if (startDate > endDate) {
        throw new Error(`Start date ${startDateStr} cannot be after end date ${endDateStr}`);
    }
    
    // Validate frequency
    if (!VALID_FREQUENCIES.includes(frequency)) {
        throw new Error(`Invalid frequency: ${frequency}. Must be one of: ${VALID_FREQUENCIES.join(', ')}`);
    }
    
    // Handle full extraction (single date range)
    if (frequency === 'full') {
        return [{
            startDate: startDateStr,
            endDate: endDateStr
        }];
    }
    
    let current = new Date(startDate);
    
    try {
        // For yearly extraction
        if (frequency === 'yearly') {
            while (current <= endDate) {
                const yearStart = new Date(current);
                const yearEnd = new Date(current.getFullYear(), 11, 31); // December 31
                
                const rangeEnd = yearEnd > endDate ? endDate : yearEnd;
                
                dateRanges.push({
                    startDate: yearStart.toISOString().split('T')[0],
                    endDate: rangeEnd.toISOString().split('T')[0]
                });
                
                current = new Date(current.getFullYear() + 1, 0, 1); // January 1 of next year
            }
        }
        // For quarterly extraction
        else if (frequency === 'quarterly') {
            while (current <= endDate) {
                const quarterStart = new Date(current);
                
                const currentMonth = current.getMonth();
                const quarterEndMonth = Math.floor(currentMonth / 3) * 3 + 2;
                const quarterEndDate = new Date(current.getFullYear(), quarterEndMonth + 1, 0);
                
                const rangeEnd = quarterEndDate > endDate ? endDate : quarterEndDate;
                
                dateRanges.push({
                    startDate: quarterStart.toISOString().split('T')[0],
                    endDate: rangeEnd.toISOString().split('T')[0]
                });
                
                current = new Date(current.getFullYear(), quarterEndMonth + 1, 1);
            }
        }
        // For monthly extraction
        else if (frequency === 'monthly') {
            while (current <= endDate) {
                const monthStart = new Date(current);
                const monthEnd = new Date(current.getFullYear(), current.getMonth() + 1, 0);
                
                const rangeEnd = monthEnd > endDate ? endDate : monthEnd;
                
                dateRanges.push({
                    startDate: monthStart.toISOString().split('T')[0],
                    endDate: rangeEnd.toISOString().split('T')[0]
                });
                
                current = new Date(current.getFullYear(), current.getMonth() + 1, 1);
            }
        }
    } catch (error) {
        throw new Error(`Error generating date ranges: ${error.message}`);
    }
    
    if (dateRanges.length === 0) {
        throw new Error('No date ranges generated');
    }
    
    return dateRanges;
}

// Enhanced key string cleaning with length limits
function cleanKeyString(str, maxLength = 50) {
    if (!str || typeof str !== 'string') {
        return 'unknown';
    }
    
    return str
        .replace(/[^a-zA-Z0-9]/g, '_')
        .replace(/_{2,}/g, '_') // Replace multiple underscores with single
        .replace(/^_+|_+$/g, '') // Remove leading/trailing underscores
        .substring(0, maxLength);
}

// Enhanced entity field extraction with fallbacks
function extractEntityFields(entity) {
    const storeName = entity.storeName || entity.StoreName || entity.store_name || entity.Store_Name;
    const extractionStartDate = entity.extractionStartDate || entity.ExtractionStartDate || entity.extraction_start_date || entity.Extraction_Start_Date;
    const extractionEndDate = entity.extractionEndDate || entity.ExtractionEndDate || entity.extraction_end_date || entity.Extraction_End_Date;
    const frequency = (entity.frequency || entity.Frequency || entity.extraction_frequency || 'monthly').toLowerCase();
    const rowKey = entity.rowKey || entity.RowKey || entity.row_key || 'unknown';
    const status = getEntityStatus(entity);
    
    return {
        storeName,
        extractionStartDate,
        extractionEndDate,
        frequency,
        rowKey,
        status
    };
}

function getEntityStatus(entity) {
    return entity.status || 
           entity.Status || 
           entity.extraction_status || 
           entity.Extraction_Status || 
           entity.extractionStatus || 
           entity.ExtractionStatus || 
           null;
}

// Process entities in batches to avoid memory issues
async function processEntitiesInBatches(entities, batchSize = MAX_ENTITIES_PER_BATCH) {
    const results = [];
    const errors = [];
    
    for (let i = 0; i < entities.length; i += batchSize) {
        const batch = entities.slice(i, i + batchSize);
        const batchResults = await processBatch(batch, i);
        
        results.push(...batchResults.success);
        errors.push(...batchResults.errors);
    }
    
    return { results, errors };
}

async function processBatch(entities, batchStartIndex) {
    const results = [];
    const errors = [];
    const startTime = Date.now();
    
    for (let i = 0; i < entities.length; i++) {
        const entity = entities[i];
        const globalIndex = batchStartIndex + i;
        
        try {
            const entityResult = await processEntity(entity, globalIndex, startTime);
            results.push(...entityResult.success);
            errors.push(...entityResult.errors);
        } catch (error) {
            errors.push({
                entityIndex: globalIndex,
                error: `Failed to process entity: ${error.message}`,
                entity: entity
            });
        }
    }
    
    return { success: results, errors };
}

async function processEntity(entity, entityIndex, startTime) {
    const results = [];
    const errors = [];
    
    try {
        const fields = extractEntityFields(entity);

        if (fields.status && fields.status.toLowerCase() === 'completed') {
            logger.warn(`Skipping completed entity that passed filter: ${fields.rowKey}`, {
                storeName: fields.storeName,
                status: fields.status
            });
            return { success: results, errors };
        }
        
        if (!fields.storeName || !fields.extractionStartDate || !fields.extractionEndDate) {
            errors.push({
                entityIndex,
                error: `Missing required fields in entity ${fields.rowKey}`,
                missingFields: {
                    storeName: !fields.storeName,
                    extractionStartDate: !fields.extractionStartDate,
                    extractionEndDate: !fields.extractionEndDate
                },
                entity
            });
            return { success: results, errors };
        }
        
        logger.info(`Processing: ${fields.storeName} (${fields.extractionStartDate} to ${fields.extractionEndDate}) with frequency: ${fields.frequency}, status: ${fields.status || 'unknown'}`);
        
        // Split date range based on frequency
        const dateRanges = splitDateRangeByFrequency(
            fields.extractionStartDate, 
            fields.extractionEndDate, 
            fields.frequency
        );
        
        // Create entries for each date range
        for (let rangeIndex = 0; rangeIndex < dateRanges.length; rangeIndex++) {
            const dateRange = dateRanges[rangeIndex];
            const uniqueId = uuidv4();
            const cleanStoreName = cleanKeyString(fields.storeName);
            
            // Create a descriptive suffix based on frequency and dates
            let dateSuffix;
            try {
                if (fields.frequency === 'yearly') {
                    dateSuffix = new Date(dateRange.startDate).getFullYear();
                } else if (fields.frequency === 'quarterly') {
                    const startMonth = new Date(dateRange.startDate).getMonth();
                    const quarter = Math.floor(startMonth / 3) + 1;
                    const year = new Date(dateRange.startDate).getFullYear();
                    dateSuffix = `${year}_Q${quarter}`;
                } else if (fields.frequency === 'monthly') {
                    const date = new Date(dateRange.startDate);
                    dateSuffix = `${date.getFullYear()}${String(date.getMonth() + 1).padStart(2, '0')}`;
                } else { // full
                    dateSuffix = `${fields.extractionStartDate.replace(/-/g, '')}_${fields.extractionEndDate.replace(/-/g, '')}`;
                }
            } catch (dateError) {
                dateSuffix = `range_${rangeIndex}`;
                logger.warn(`Failed to generate date suffix, using fallback: ${dateError.message}`);
            }
            
            const newEntity = {
                partitionKey: cleanStoreName,
                rowKey: `${cleanStoreName}_${dateSuffix}_${uniqueId}`,
                storeName: fields.storeName,
                extractionStartDate: dateRange.startDate,
                extractionEndDate: dateRange.endDate,
                status: 'open',
                count: '0',
                rangeSequence: rangeIndex + 1,
                totalRanges: dateRanges.length,
                frequency: fields.frequency,
                createdDate: new Date().toISOString(),
                updatedDate: new Date().toISOString(),
                sourceRowKey: fields.rowKey,
                processingTimestamp: new Date(startTime).toISOString(),
                executionId: uuidv4()
            };
            
            // Insert entity with retry logic
            try {
                await retryOperation(async () => {
                    await targetTableClient.createEntity(newEntity);
                });
                
                results.push({
                    storeName: fields.storeName,
                    startDate: dateRange.startDate,
                    endDate: dateRange.endDate,
                    rangeSequence: rangeIndex + 1,
                    totalRanges: dateRanges.length,
                    frequency: fields.frequency,
                    rowKey: newEntity.rowKey,
                    partitionKey: newEntity.partitionKey,
                    created: true
                });
                
                logger.info(`Created: ${fields.storeName} - ${dateRange.startDate} to ${dateRange.endDate} (${fields.frequency})`);
                
            } catch (insertError) {
                if (insertError.statusCode === 409) {
                    // Entity already exists
                    logger.warn(`Entity already exists: ${newEntity.rowKey}`);
                    results.push({
                        storeName: fields.storeName,
                        startDate: dateRange.startDate,
                        endDate: dateRange.endDate,
                        rangeSequence: rangeIndex + 1,
                        totalRanges: dateRanges.length,
                        frequency: fields.frequency,
                        rowKey: newEntity.rowKey,
                        partitionKey: newEntity.partitionKey,
                        alreadyExists: true
                    });
                } else {
                    throw insertError;
                }
            }
        }
        
    } catch (entityError) {
        errors.push({
            entityIndex,
            error: `Error processing entity ${fields?.rowKey || 'unknown'}: ${entityError.message}`,
            entity
        });
    }
    
    return { success: results, errors };
}

// Main handler function
app.http("getExtractionDateByStore", {
    methods: ["POST"],
    authLevel: "function",
    route: "getExtractionDateByStore",
    handler: async (request, context) => {
        context.log("Get Extraction Date By Store function processed a request.");

        try {
            await initializeConfiguration();
            const result = await getExtractionDateByStore(request, context);
            
            return {
                status: result.status,
                headers: {
                    "Content-Type": "application/json",
                },
                body: result.body, // Already a string
            };
        } catch (error) {
            context.log("Error in getExtractionDateByStore:", error);
            return {
                status: 500,
                headers: {
                    "Content-Type": "application/json",
                },
                body: JSON.stringify({ 
                    success: false,
                    error: "Internal server error",
                    message: error.message,
                    timestamp: new Date().toISOString()
                }),
            };
        }
    },
});

async function getExtractionDateByStore(request, context) {
    if (!context) {
        throw new Error("Azure Functions context is required");
    }

    if (!config) {
        await initializeConfiguration();
    }

    const executionId = uuidv4();
    const startTime = new Date();

    logger = createAzureFunctionLogger(context, config);

    logger.azureInfo("Execution started with file logging enabled", {
        executionId,
        logFile: logger.logFilePath,
        logsDirectory: logger.logsDir,
        timestamp: startTime.toISOString(),
    });

    let response = {
        status: 500,
        body: JSON.stringify({
            executionId,
            success: false,
            error: "Unknown error occurred",
            timestamp: startTime.toISOString(),
            logFile: logger.logFilePath,
        }),
    };

    try {
        logger.azureInfo("Store extraction date processing started", {
            executionId,
            timestamp: startTime.toISOString(),
        });

        // Parse and validate request body
        let requestBody = {};
        try {
            if (typeof request.json === "function") {
                requestBody = await request.json();
            } else if (request.body) {
                if (typeof request.body === "string") {
                    requestBody = JSON.parse(request.body);
                } else {
                    requestBody = request.body;
                }
            }
        } catch (parseError) {
            logger.warn("Failed to parse request body", {
                error: parseError.message,
            });
            throw new Error(`Invalid request body: ${parseError.message}`);
        }

        // Validate and sanitize input
        const { errors: validationErrors, sanitized } = validateAndSanitizeInput(requestBody);
        if (validationErrors.length > 0) {
            throw new Error(`Input validation failed: ${validationErrors.join(', ')}`);
        }

        const {
            storeName: specificStore,
            validateData = true,
            health = false
        } = sanitized;

        // Log the parsed request
        logger.info("Request parameters parsed", {
            executionId,
            specificStore,
            validateData,
            health
        });

        // Initialize table clients
        initializeTableClients();
        
        // Test connection by attempting to create tables if they don't exist
        await retryOperation(async () => {
            try {
                await sourceTableClient.createTable();
            } catch (error) {
                if (error.statusCode !== 409) { // 409 = table already exists
                    throw new Error(`Cannot access source table: ${error.message}`);
                }
            }
        });
        
        await retryOperation(async () => {
            try {
                await targetTableClient.createTable();
            } catch (error) {
                if (error.statusCode !== 409) { // 409 = table already exists
                    throw new Error(`Cannot access target table: ${error.message}`);
                }
            }
        });

        // Check if this is a health check request
        if (health) {
            const healthResponse = {
                status: 'healthy',
                timestamp: new Date().toISOString(),
                function: 'getExtractionDateByStore',
                version: '1.0.0',
                tablesAccessible: true,
                configurationLoaded: isConfigurationLoaded,
                executionId
            };

            response = {
                status: 200,
                body: JSON.stringify(healthResponse),
            };

            return response;
        }

        // Set up filter for specific store processing
        let filter = null;
        const statusFilter = "(status ne 'Completed' and status ne 'completed') or (Status ne 'Completed' and Status ne 'completed')";
        if (specificStore) {
            logger.info(`Processing specific store: ${specificStore} (excluding completed records)`);
            // Use the sanitized store name for the filter
            const storeFilter = `storeName eq '${specificStore}' or StoreName eq '${specificStore}'`;
            filter = `(${storeFilter}) and (${statusFilter})`;
        } else {
            // Just filter by status when no specific store
            filter = statusFilter;
        }

        logger.info('Applied filter to exclude completed records', { 
            filter: filter,
            specificStore: specificStore || 'all stores'
        });

        // Read entities from source table with streaming to handle large datasets
         logger.info('Reading from StoreExtractionLog table (excluding completed records)...', {
            filter: filter,
            excludingCompleted: true
        });
        const sourceEntities = [];
        
        const listOptions = filter ? { queryOptions: { filter } } : {};
        
        try {
            for await (const entity of sourceTableClient.listEntities(listOptions)) {
                sourceEntities.push(entity);
                
                // Add safety limit to prevent memory issues
                if (sourceEntities.length > 10000) {
                    logger.warn('Large dataset detected, processing first 10,000 entities');
                    break;
                }
            }
        } catch (listError) {
            throw new Error(`Failed to read from source table: ${listError.message}`);
        }
        
        if (sourceEntities.length === 0) {
            const message = specificStore ? 
                `No non-completed records found for store: ${specificStore}` : 
                'No non-completed records found in StoreExtractionLog table';
            
            response = {
                status: 200,
                body: JSON.stringify({
                    executionId,
                    success: true,
                    message: message,
                    processedCount: 0,
                    excludedCompleted: true,
                    filter: filter,
                    processingTime: `${Date.now() - startTime.getTime()}ms`,
                    results: [],
                    errors: [],
                    timestamp: new Date().toISOString()
                }),
            };
            return response;
        }
        
        logger.info(`Found ${sourceEntities.length} non-completed records to process`, {
            totalFound: sourceEntities.length,
            excludedCompleted: true,
            filter: filter
        });

        const results = {
            executionId,
            startTime: startTime.toISOString(),
            step1: { sourceRecords: sourceEntities.length, processedRecords: 0 },
            step2: { dateRangesCreated: 0, recordsInserted: 0 },
            step3: { dataValidation: null },
            success: false,
            errors: [],
            results: [],
            logFile: logger.logFilePath,
        };

        // Step 1: Process entities in batches
        logger.info("Step 1: Processing source entities and creating date ranges");
        
        const { results: processingResults, errors: processingErrors } = await processEntitiesInBatches(sourceEntities);

        results.step1.processedRecords = sourceEntities.length;
        results.step2.dateRangesCreated = processingResults.length;
        results.step2.recordsInserted = processingResults.filter(r => r.created).length;
        results.results = processingResults.slice(0, 100); // Limit results for response size
        results.errors = processingErrors.slice(0, 10); // Limit errors for response size

        logger.info("Step 1 completed: Entity processing finished", {
            sourceRecords: sourceEntities.length,
            dateRangesCreated: processingResults.length,
            recordsInserted: processingResults.filter(r => r.created).length,
            errors: processingErrors.length
        });

        // Step 3: Data validation
        if (validateData && sourceEntities.length > 0) {
            try {
                logger.info("Step 3: Validating data consistency");
                
                const totalInserted = processingResults.filter(r => r.created).length;
                const totalErrors = processingErrors.length;
                
                const dataValidation = {
                    sourceRecordsFound: sourceEntities.length,
                    dateRangesCreated: processingResults.length,
                    recordsInserted: totalInserted,
                    recordsAlreadyExisted: processingResults.filter(r => r.alreadyExists).length,
                    errors: totalErrors,
                    isValid: totalErrors === 0 && totalInserted > 0,
                    message: totalErrors === 0 && totalInserted > 0 
                        ? "Data processing completed successfully"
                        : `Processing completed with ${totalErrors} errors and ${totalInserted} successful insertions`,
                    processingTimestamp: new Date().toISOString()
                };
                
                results.step3.dataValidation = dataValidation;
                
                logger.info("Step 3 completed: Data validation " + 
                    (dataValidation.isValid ? "passed" : "completed with warnings"), {
                    validation: dataValidation.message,
                    sourceRecords: dataValidation.sourceRecordsFound,
                    dateRangesCreated: dataValidation.dateRangesCreated,
                    recordsInserted: dataValidation.recordsInserted,
                    recordsAlreadyExisted: dataValidation.recordsAlreadyExisted,
                    errors: dataValidation.errors
                });
                
                if (!dataValidation.isValid && totalErrors > 0) {
                    results.errors.push({
                        step: 3,
                        error: "Data validation found processing errors: " + dataValidation.message,
                        severity: "warning"
                    });
                }
                
            } catch (error) {
                logger.error("Step 3 failed", { error: error.message });
                results.errors.push({ step: 3, error: error.message });
            }
        } else {
            logger.info("Step 3 skipped: Data validation disabled or no source data");
        }

        const endTime = new Date();
        results.endTime = endTime.toISOString();
        results.duration = endTime.getTime() - startTime.getTime();
        
        // Determine success based on errors (excluding warnings)
        const criticalErrors = results.errors.filter((e) => e.severity !== "warning");
        results.success = criticalErrors.length === 0 && processingErrors.length < sourceEntities.length;

        // Prepare final summary
        const summary = {
            sourceRecordsFound: sourceEntities.length,
            targetRecordsCreated: processingResults.filter(r => r.created).length,
            targetRecordsAlreadyExisted: processingResults.filter(r => r.alreadyExists).length,
            errors: processingErrors.length,
            processingTimeMs: results.duration,
            processedStore: specificStore || 'all stores',
            executionId: executionId
        };

        logger.azureInfo("Extraction date processing completed", {
            executionId,
            duration: Math.round(results.duration / 1000) + "s",
            success: results.success,
            logFile: logger.logFilePath,
            summary: summary
        });

        response = {
            status: results.success ? 200 : 207,
            body: JSON.stringify({
                ...results,
                summary: summary
            }),
        };

    } catch (error) {
        logger.error("Extraction date processing failed", {
            executionId,
            error: error.message,
            stack: error.stack,
        });

        response = {
            status: 500,
            body: JSON.stringify({
                executionId,
                success: false,
                error: error.message,
                timestamp: new Date().toISOString(),
                duration: new Date().getTime() - startTime.getTime(),
                logFile: logger.logFilePath,
            }),
        };
    }

    return response;
}
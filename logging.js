const winston = require('winston');
const path = require('path');
const fs = require('fs').promises;

// Enhanced logging configuration with file output
function createLogger(config) {
  // Create logs directory if it doesn't exist
  const logsDir = path.join(process.cwd(), 'logs');
  
  // Ensure logs directory exists (synchronous for initialization)
  try {
    require('fs').mkdirSync(logsDir, { recursive: true });
  } catch (error) {
    // Directory might already exist, ignore error
  }

  // Generate log file name with timestamp
  const timestamp = new Date().toISOString().replace(/[:.]/g, '-').slice(0, 19);
  const logFileName = `shopify-bulk-extraction-${timestamp}.log`;
  const logFilePath = path.join(logsDir, logFileName);

  // Custom format for file logging
  const fileFormat = winston.format.combine(
    winston.format.timestamp({ format: 'YYYY-MM-DDTHH:mm:ss.SSSZ' }),
    winston.format.errors({ stack: true }),
    winston.format.printf(({ timestamp, level, message, ...meta }) => {
      const metaString = Object.keys(meta).length ? JSON.stringify(meta, null, 2) : '';
      return `[${timestamp}] [${level.toUpperCase()}] ${message}${metaString ? '\n' + metaString : ''}`;
    })
  );

  // Custom format for console logging (more compact)
  const consoleFormat = winston.format.combine(
    winston.format.timestamp({ format: 'YYYY-MM-DDTHH:mm:ss.SSSZ' }),
    winston.format.errors({ stack: true }),
    winston.format.printf(({ timestamp, level, message, ...meta }) => {
      const metaString = Object.keys(meta).length ? ' ' + JSON.stringify(meta) : '';
      return `[${timestamp}] [${level.toUpperCase()}] ${message}${metaString}`;
    })
  );

  const logger = winston.createLogger({
    level: config.LOG_LEVEL || 'info',
    transports: [
      // Console transport (existing)
      new winston.transports.Console({
        format: consoleFormat
      }),
      
      // File transport (new)
      new winston.transports.File({
        filename: logFilePath,
        format: fileFormat,
        maxsize: 50 * 1024 * 1024, // 50MB max file size
        maxFiles: 10, // Keep up to 10 log files
        tailable: true
      }),
      
      // Error-only file transport
      new winston.transports.File({
        filename: path.join(logsDir, `shopify-bulk-extraction-errors-${timestamp}.log`),
        level: 'error',
        format: fileFormat,
        maxsize: 10 * 1024 * 1024, // 10MB max file size
        maxFiles: 5
      })
    ]
  });

  // Add log file info to the logger for reference
  logger.logFilePath = logFilePath;
  logger.logsDir = logsDir;
  
  // Log the initialization
  logger.info('Logger initialized with file output', {
    logFilePath,
    logsDir,
    logLevel: config.LOG_LEVEL || 'info'
  });

  return logger;
}

// Alternative: Daily rotating file logs
function createLoggerWithRotation(config) {
  const DailyRotateFile = require('winston-daily-rotate-file');
  
  const logsDir = path.join(process.cwd(), 'logs');
  
  try {
    require('fs').mkdirSync(logsDir, { recursive: true });
  } catch (error) {
    // Directory might already exist
  }

  const fileFormat = winston.format.combine(
    winston.format.timestamp({ format: 'YYYY-MM-DDTHH:mm:ss.SSSZ' }),
    winston.format.errors({ stack: true }),
    winston.format.printf(({ timestamp, level, message, ...meta }) => {
      const metaString = Object.keys(meta).length ? JSON.stringify(meta, null, 2) : '';
      return `[${timestamp}] [${level.toUpperCase()}] ${message}${metaString ? '\n' + metaString : ''}`;
    })
  );

  const consoleFormat = winston.format.combine(
    winston.format.timestamp({ format: 'YYYY-MM-DDTHH:mm:ss.SSSZ' }),
    winston.format.errors({ stack: true }),
    winston.format.printf(({ timestamp, level, message, ...meta }) => {
      const metaString = Object.keys(meta).length ? ' ' + JSON.stringify(meta) : '';
      return `[${timestamp}] [${level.toUpperCase()}] ${message}${metaString}`;
    })
  );

  const logger = winston.createLogger({
    level: config.LOG_LEVEL || 'info',
    transports: [
      // Console transport
      new winston.transports.Console({
        format: consoleFormat
      }),
      
      // Daily rotating file for all logs
      new DailyRotateFile({
        filename: path.join(logsDir, 'shopify-bulk-extraction-%DATE%.log'),
        datePattern: 'YYYY-MM-DD',
        zippedArchive: true,
        maxSize: '50m',
        maxFiles: '30d', // Keep 30 days
        format: fileFormat
      }),
      
      // Daily rotating file for errors only
      new DailyRotateFile({
        filename: path.join(logsDir, 'shopify-bulk-extraction-errors-%DATE%.log'),
        datePattern: 'YYYY-MM-DD',
        level: 'error',
        zippedArchive: true,
        maxSize: '10m',
        maxFiles: '60d', // Keep errors for 60 days
        format: fileFormat
      })
    ]
  });

  logger.logsDir = logsDir;
  
  logger.info('Logger initialized with daily rotation', {
    logsDir,
    logLevel: config.LOG_LEVEL || 'info',
    rotation: 'daily'
  });

  return logger;
}

// Enhanced Azure Functions specific logger
function createAzureFunctionLogger(context, config) {
  const logsDir = path.join(process.cwd(), 'logs');
  
  try {
    require('fs').mkdirSync(logsDir, { recursive: true });
  } catch (error) {
    // Directory might already exist
  }

  const timestamp = new Date().toISOString().replace(/[:.]/g, '-').slice(0, 19);
  const logFileName = `azure-function-${timestamp}.log`;
  const logFilePath = path.join(logsDir, logFileName);

  // Custom format that includes execution context
  const fileFormat = winston.format.combine(
    winston.format.timestamp({ format: 'YYYY-MM-DDTHH:mm:ss.SSSZ' }),
    winston.format.errors({ stack: true }),
    winston.format.printf(({ timestamp, level, message, ...meta }) => {
      const executionContext = {
        invocationId: context?.invocationId,
        functionName: context?.executionContext?.functionName,
        functionDirectory: context?.executionContext?.functionDirectory
      };
      
      const combinedMeta = { ...executionContext, ...meta };
      const metaString = Object.keys(combinedMeta).length ? JSON.stringify(combinedMeta, null, 2) : '';
      return `[${timestamp}] [${level.toUpperCase()}] ${message}${metaString ? '\n' + metaString : ''}`;
    })
  );

  const consoleFormat = winston.format.combine(
    winston.format.timestamp({ format: 'YYYY-MM-DDTHH:mm:ss.SSSZ' }),
    winston.format.errors({ stack: true }),
    winston.format.printf(({ timestamp, level, message, ...meta }) => {
      const metaString = Object.keys(meta).length ? ' ' + JSON.stringify(meta) : '';
      return `[${timestamp}] [${level.toUpperCase()}] ${message}${metaString}`;
    })
  );

  const logger = winston.createLogger({
    level: config.LOG_LEVEL || 'info',
    transports: [
      new winston.transports.Console({
        format: consoleFormat
      }),
      new winston.transports.File({
        filename: logFilePath,
        format: fileFormat,
        maxsize: 100 * 1024 * 1024, // 100MB for Azure Functions
        maxFiles: 5
      })
    ]
  });

  // Add Azure-specific methods
  logger.azureInfo = (message, meta = {}) => {
    logger.info(message, { ...meta, source: 'azure-function' });
  };

  logger.azureError = (message, error, meta = {}) => {
    logger.error(message, { 
      ...meta, 
      error: error?.message, 
      stack: error?.stack,
      source: 'azure-function' 
    });
  };

  logger.logFilePath = logFilePath;
  logger.logsDir = logsDir;

  logger.info('Azure Function logger initialized', {
    logFilePath,
    invocationId: context?.invocationId,
    functionName: context?.executionContext?.functionName
  });

  return logger;
}

module.exports = {
  createLogger,
  createLoggerWithRotation,
  createAzureFunctionLogger
};
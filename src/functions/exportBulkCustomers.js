const { app } = require("@azure/functions");
const axios = require("axios");
const winston = require("winston");
const { v4: uuidv4 } = require("uuid");
const fs = require("fs").promises;
const path = require("path");
const { createAzureFunctionLogger } = require("../../logging");
const { BlobServiceClient } = require("@azure/storage-blob");
const { loadConfig } = require("../../config-loader");

let config = null;
let logger = null;
const minimumRetryDelay = 1000; // Minimum retry delay in milliseconds
const maxFallBackTime = 60000; // Maximum fallback time in milliseconds 60 sec

const CUSTOMER_NODE_FIELDS = `
              id
              legacyResourceId
              displayName
              firstName
              lastName
              email
              phone
              state
              tags
              note
              verifiedEmail
              emailMarketingConsent {
                marketingState
                consentUpdatedAt
              }
              smsMarketingConsent {
                marketingOptInLevel
                marketingState
                consentUpdatedAt
              }
              taxExempt
              createdAt
              updatedAt
              numberOfOrders
              lifetimeDuration
              locale
              defaultAddress {
                id
                address1
                address2
                city
                company
                country
                countryCodeV2
                firstName
                lastName
                phone
                province
                provinceCode
                zip
              }
              addresses {
                id
                address1
                address2
                city
                company
                country
                countryCodeV2
                firstName
                lastName
                phone
                province
                provinceCode
                zip
              }
              amountSpent {
                amount
                currencyCode
              }
              image {
                id
                url
                altText
              }`;

const MINIMAL_NODE_FIELDS = `
              id
              legacyResourceId`;

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

app.http("exportBulkCustomers", {
  methods: ["POST"],
  authLevel: "function",
  route: "exportBulkCustomers",
  handler: async (request, context) => {
    context.log("Export Bulk Customers function processed a request.");

    try {
      await initializeConfiguration();
      const result = await exportBulkCustomers(request, context);
      return {
        status: 200,
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify(result),
      };
    } catch (error) {
      context.log("Error in exportBulkCustomers:", error);
      return {
        status: 500,
        body: JSON.stringify({ error: "Internal server error" }),
      };
    }
  },
});

function validateApproach(rawApproach) {
  const validApproaches = ["tag-based", "full", "date-based"];

  if (!rawApproach) {
    throw new Error(
      "Approach parameter is required. Valid approaches are: " +
        validApproaches.join(", ")
    );
  }

  const approach = rawApproach.toLowerCase().trim();

  if (!validApproaches.includes(approach)) {
    throw new Error(
      `Invalid approach: "${rawApproach}". Valid approaches are: ${validApproaches.join(
        ", "
      )}`
    );
  }

  return approach;
}

async function exportBulkCustomers(request, context) {
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
    headers: { "Content-Type": "application/json" },
    body: {
      executionId,
      success: false,
      error: "Unknown error occurred",
      timestamp: startTime.toISOString(),
      logFile: logger.logFilePath,
    },
  };

  try {
    logger.azureInfo("Shopify bulk customer extraction started", {
      executionId,
      timestamp: startTime.toISOString(),
    });

    let requestBody = {};
    try {
      if (typeof request.json === "function") {
        try {
          requestBody = await request.json();
          logger.info("Parsed body using request.json()", { requestBody });
        } catch (jsonError) {
          logger.warn("request.json() failed, trying alternatives", {
            error: jsonError.message,
          });
          if (request.body && typeof request.body === "string") {
            requestBody = JSON.parse(request.body);
            logger.info("Parsed body using JSON.parse(request.body)", {
              requestBody,
            });
          } else if (request.body) {
            requestBody = request.body;
            logger.info("Using request.body directly", { requestBody });
          } else {
            throw new Error("No valid request body found");
          }
        }
      } else if (request.body) {
        if (typeof request.body === "string") {
          requestBody = JSON.parse(request.body);
        } else {
          requestBody = request.body;
        }
        logger.info("Parsed body using fallback method", { requestBody });
      } else {
        throw new Error("No request body found");
      }

      logger.info("Final parsed request body", {
        requestBody,
        keys: Object.keys(requestBody),
        hasApproach: !!requestBody.approach,
        approach: requestBody.approach,
      });
    } catch (parseError) {
      logger.error("All request body parsing methods failed", {
        error: parseError.message,
        requestHasBody: !!request.body,
        requestBodyType: typeof request.body,
        requestBodyValue: request.body,
        requestHasJson: typeof request.json === "function",
      });
      requestBody = {};
      logger.warn("Using default request body due to parsing failure", {
        requestBody,
      });
    }

    const {
      tag: customTag,
      skipTagging = false,
      skipExtraction = false,
      cancelExisting = false,
      approach: rawApproach,
      validateData = true,
      startDate = null,
      endDate = null,
      removeTagsAfterUpload = false,
    } = requestBody;

    logger.info("Request parameters parsed", {
      executionId,
      customTag,
      skipTagging,
      skipExtraction,
      cancelExisting,
      approach: rawApproach,
      validateData,
      startDate,
      endDate,
    });

    const selectedApproach = validateApproach(rawApproach);

    if (selectedApproach === "date-based") {
      if (!startDate || !endDate) {
        throw new Error(
          "Date-based approach requires both startDate and endDate parameters."
        );
      }
      const start = new Date(startDate);
      if (isNaN(start.getTime())) {
        throw new Error("Invalid startDate format. Use ISO format.");
      }
      const end = new Date(endDate);
      if (isNaN(end.getTime())) {
        throw new Error("Invalid endDate format. Use ISO format.");
      }
      if (start > end) {
        throw new Error("startDate must be before or equal to endDate");
      }
      logger.info("Date range validation passed", {
        startDate: start.toISOString(),
        endDate: end.toISOString(),
        rangeDays: Math.ceil((end - start) / (1000 * 60 * 60 * 24)),
      });
    }

    logger.info("Execution approach validated: " + selectedApproach, {
      approach: selectedApproach,
      dateFilter:
        selectedApproach === "date-based"
          ? { startDate, endDate }
          : "not applicable",
    });

    const shopifyAPI = new ShopifyBulkAPI();

    const results = {
      executionId,
      approach: selectedApproach,
      startTime: startTime.toISOString(),
      step1: { customerIds: [], totalCount: 0, blobResult: null },
      step2: { taggingResults: null },
      step3: { extractionMethod: null, blobResult: null, totalCount: 0 },
      step4: { azureBlobFile: null },
      step4_5: { tagRemovalResults: null, cleanupCompleted: false },
      step5: { dataValidation: null },
      downloadUrl: null,
      success: false,
      errors: [],
      logFile: logger.logFilePath,
    };

    if (cancelExisting) {
      try {
        logger.info("Canceling existing bulk operation");
        const cancelResult = await shopifyAPI.cancelCurrentBulkOperation();
        if (cancelResult) {
          logger.info("Successfully cancelled existing bulk operation", {
            operationId: cancelResult.id,
            status: cancelResult.status,
          });
          await shopifyAPI.delay(10000);
        } else {
          logger.info("No bulk operation was cancelled");
        }
      } catch (error) {
        logger.warn("Failed to cancel existing bulk operation", {
          error: error.message,
        });
      }
    }

    // Step 1: Extract customer IDs (only for tag-based approach)
    if (selectedApproach === "tag-based" && !skipTagging) {
      try {
        logger.info("Step 1: Extracting customer IDs with basic customer data");
        const customerResult = await shopifyAPI.getAllCustomerIds(executionId);
        results.step1.customerIds = customerResult.customerIds;
        results.step1.totalCount = customerResult.customerIds.length;
        results.step1.blobResult = customerResult.blobResult;

        logger.info(
          "Step 1 completed: Streamed customer IDs to Azure Blob Storage",
          {
            blobPath: customerResult.blobResult?.blobPath,
            blobUrl: customerResult.blobResult?.blobUrl,
            fields: "MINIMAL_NODE_FIELDS (id, legacyResourceId)",
          }
        );

        if (
          customerResult.customerIds.length === 0 &&
          !customerResult.blobResult
        ) {
          logger.info("No customers found, ending process");
          results.success = true;
          response = {
            status: 200,
            headers: { "Content-Type": "application/json" },
            body: results,
          };
          context.res = response;
          return response;
        }
      } catch (error) {
        logger.error("Step 1 failed", { error: error.message });
        results.errors.push({ step: 1, error: error.message });
        throw error;
      }
    } else {
      logger.info(
        "Step 1 skipped: Not required for " + selectedApproach + " approach"
      );
    }

    // Step 2: Tagging
    if (
      selectedApproach === "tag-based" &&
      !skipTagging &&
      results.step1.customerIds.length > 0
    ) {
      try {
        logger.info("Step 2a: Initial bulk tagging attempt");
        const initialTaggingResults = await shopifyAPI.bulkTagCustomers(
          results.step1.customerIds,
          customTag,
          null
        );

        results.step2.taggingResults = {
          successful: initialTaggingResults.successful,
          failed: initialTaggingResults.failed,
          errors: initialTaggingResults.errors || [],
        };
        results.step2.initialTaggingResults = initialTaggingResults;

        logger.info("Step 2a completed: Initial tagging results", {
          successful: initialTaggingResults.successful,
          failed: initialTaggingResults.failed,
          total: results.step1.customerIds.length,
          successRate:
            (
              (initialTaggingResults.successful /
                results.step1.customerIds.length) *
              100
            ).toFixed(1) + "%",
        });

        if (initialTaggingResults.failed > 0) {
          logger.info(
            "Step 2b: Using negative tag query to identify and tag remaining customers"
          );
          const untaggedCustomers = await shopifyAPI.getCustomersWithoutTag(
            customTag,
            executionId
          );
          const untaggedCustomerIds = untaggedCustomers
            .filter((customer) => customer.legacyResourceId)
            .map((customer) => customer.legacyResourceId);

          if (untaggedCustomerIds.length > 0) {
            logger.info(
              `Step 2b: Tagging ${untaggedCustomerIds.length} remaining customers`
            );
            const remainingTaggingResults = await shopifyAPI.bulkTagCustomers(
              untaggedCustomerIds,
              customTag,
              null
            );

            const finalSuccessful =
              initialTaggingResults.successful +
              remainingTaggingResults.successful;
            const finalFailed = remainingTaggingResults.failed;
            const finalSuccessRate = (
              (finalSuccessful / results.step1.customerIds.length) *
              100
            ).toFixed(1);

            results.step2.negativeQueryResults = {
              untaggedFound: untaggedCustomers.length,
              untaggedCustomerIds: untaggedCustomerIds.length,
              additionalTagged: remainingTaggingResults.successful,
              stillFailed: remainingTaggingResults.failed,
              method: "negative_tag_query",
            };

            results.step2.taggingResults.finalSuccessful = finalSuccessful;
            results.step2.taggingResults.finalFailed = finalFailed;
            results.step2.taggingResults.finalSuccessRate = finalSuccessRate;

            logger.info(
              "Step 2b completed: Negative tag query approach results",
              {
                initialSuccessful: initialTaggingResults.successful,
                additionalTagged: remainingTaggingResults.successful,
                finalSuccessful,
                finalFailed,
                finalSuccessRate: finalSuccessRate + "%",
              }
            );
          } else {
            logger.info(
              "All untagged customers lack legacyResourceId - using initial results"
            );
            results.step2.taggingResults.finalSuccessful =
              initialTaggingResults.successful;
            results.step2.taggingResults.finalFailed =
              initialTaggingResults.failed;
          }
        } else {
          logger.info(
            "Step 2: Perfect tagging success - no negative query needed"
          );
          results.step2.taggingResults.finalSuccessful =
            initialTaggingResults.successful;
          results.step2.taggingResults.finalFailed = 0;
          results.step2.taggingResults.finalSuccessRate = "100.0";
        }

        const finalSuccessful = results.step2.taggingResults.finalSuccessful;
        const finalFailed = results.step2.taggingResults.finalFailed || 0;
        const finalSuccessRate = parseFloat(
          results.step2.taggingResults.finalSuccessRate || "0"
        );
        const minimumAcceptableRate = 95;

        if (finalSuccessRate >= minimumAcceptableRate) {
          logger.info(
            `Step 2 validation passed: ${finalSuccessRate}% success rate meets ${minimumAcceptableRate}% threshold`,
            {
              finalSuccessful,
              finalFailed,
              decision: "PROCEED to Step 3",
            }
          );
        } else {
          logger.warn(
            `Step 2 partial success: ${finalSuccessRate}% success rate below ${minimumAcceptableRate}% threshold`,
            {
              finalSuccessful,
              finalFailed,
              decision: "PROCEED to Step 3 with warning",
            }
          );
          results.errors.push({
            step: 2,
            error: `Tagging success rate (${finalSuccessRate}%) below optimal threshold (${minimumAcceptableRate}%).`,
            severity: "warning",
          });
        }
      } catch (error) {
        logger.error("Step 2 failed with critical exception", {
          error: error.message,
          stack: error.stack,
        });
        results.errors.push({
          step: 2,
          error: error.message,
          severity: "critical",
          fallbackStrategy: "Will attempt full extraction instead of tag-based",
        });
        results.step2.useFallbackStrategy = true;
        results.step2.taggingResults = {
          successful: 0,
          failed: results.step1.customerIds.length,
          errors: [{ error: error.message }],
          finalSuccessful: 0,
          finalFailed: results.step1.customerIds.length,
          finalSuccessRate: "0",
        };
      }
    } else {
      logger.info(
        "Step 2 skipped: Tagging not required for " + selectedApproach
      );
      results.step2.taggingResults = {
        successful: 0,
        failed: 0,
        errors: [],
        finalSuccessful: 0,
        finalFailed: 0,
        finalSuccessRate: "0",
        skipped: true,
      };
    }

    // Step 3: Extraction
    if (!skipExtraction) {
      try {
        let blobResult = null;
        let extractionMethod = selectedApproach;
        let fallbackReason = null;

        if (selectedApproach === "tag-based") {
          if (results.step2.useFallbackStrategy) {
            logger.info(
              "Step 3: Using fallback strategy - Full customer data extraction"
            );
            const result = await shopifyAPI.getAllCustomersFullData(
              executionId
            );
            blobResult = result.blobResult;
            extractionMethod = "full-fallback";
            fallbackReason = "Step 2 tagging failed critically";
          } else {
            const finalSuccessful =
              results.step2.taggingResults?.finalSuccessful || 0;
            const finalFailed = results.step2.taggingResults?.finalFailed || 0;
            const totalAttempted = finalSuccessful + finalFailed;
            const successRate =
              totalAttempted > 0
                ? (finalSuccessful / totalAttempted) * 100
                : 100;

            logger.info("Step 3: Evaluating extraction strategy", {
              finalSuccessful,
              finalFailed,
              totalAttempted,
              successRate: successRate.toFixed(1) + "%",
            });

            if (finalSuccessful > 0) {
              const tagPropagationDelay = Math.min(
                10000,
                Math.max(3000, finalSuccessful * 200)
              ); // 200ms per customer, min 3s, max 10s
              logger.info(
                "Step 3: Waiting for tag propagation in Shopify's search index",
                {
                  taggedCustomers: finalSuccessful,
                  propagationDelayMs: tagPropagationDelay,
                  reason:
                    "Tags need time to be indexed before search queries can find them",
                }
              );
              await shopifyAPI.delay(tagPropagationDelay);
            }

            try {
              logger.info(
                "Step 3: Attempting tag-based extraction with tag: " + customTag
              );
              const result = await shopifyAPI.extractCustomersByTag(
                customTag,
                executionId
              );
              blobResult = result.blobResult;
              extractionMethod = "tag-based";

              if (!blobResult || !blobResult.success) {
                throw new Error(
                  "Tag-based extraction failed - no valid blob result"
                );
              }

              logger.info("Step 3: Tag-based extraction successful", {
                blobPath: blobResult.blobPath,
                sizeMB: Math.round(blobResult.sizeBytes / (1024 * 1024)),
                method: "tag-based",
              });
            } catch (tagExtractionError) {
              logger.warn(
                "Step 3: Tag-based extraction failed, trying fallback approaches",
                {
                  error: tagExtractionError.message,
                  fallbackStrategy: "full extraction due to tag search issues",
                }
              );

              try {
                logger.info(
                  "Step 3: Using full extraction fallback due to tag search issues"
                );
                const result = await shopifyAPI.getAllCustomersFullData(
                  executionId
                );
                blobResult = result.blobResult;
                extractionMethod = "full-fallback";
                fallbackReason =
                  "Tag-based extraction failed, likely due to search index timing issues";

                if (!blobResult || !blobResult.success) {
                  throw new Error("Full extraction fallback also failed");
                }

                logger.info("Step 3: Full extraction fallback successful", {
                  blobPath: blobResult.blobPath,
                  sizeMB: Math.round(blobResult.sizeBytes / (1024 * 1024)),
                  method: "full-fallback",
                  reason: "tag-search-timing-issues",
                });
              } catch (fullExtractionError) {
                logger.error("Step 3: Full extraction fallback failed", {
                  error: fullExtractionError.message,
                });
                throw fullExtractionError;
              }
            }
          }
        } else if (selectedApproach === "full") {
          logger.info("Step 3: Full customer data extraction");
          const result = await shopifyAPI.getAllCustomersFullData(executionId);
          blobResult = result.blobResult;
          extractionMethod = "full";
        } else if (selectedApproach === "date-based") {
          logger.info("Step 3: Date-filtered customer data extraction", {
            startDate,
            endDate,
          });
          const result = await shopifyAPI.getAllCustomersFullDataWithDateFilter(
            startDate,
            endDate,
            executionId
          );
          blobResult = result.blobResult;
          extractionMethod = "date-based";
          results.step3.dateFilter = { startDate, endDate };
        }

        results.step3.extractionMethod = extractionMethod;
        results.step3.blobResult = blobResult;
        if (fallbackReason) {
          results.step3.fallbackReason = fallbackReason;
        }
        results.downloadUrl = blobResult?.blobUrl;

        logger.info(
          "Step 3 completed: Streamed customer data to Azure Blob Storage",
          {
            blobPath: blobResult?.blobPath,
            blobUrl: blobResult?.blobUrl,
            extractionMethod,
            fields: "CUSTOMER_NODE_FIELDS", // Always full fields in Step 3
            success: blobResult?.success || false,
          }
        );

        // CRITICAL FIX: Don't fall back to Step 1 if Step 3 succeeds with valid blob
        if (!blobResult || !blobResult.success) {
          throw new Error(
            "No valid blob generated in Step 3 - all extraction methods failed"
          );
        }
      } catch (error) {
        logger.error("Step 3 failed with all extraction methods", {
          error: error.message,
        });
        results.errors.push({ step: 3, error: error.message });

        // LAST RESORT: Try one more full extraction attempt
        try {
          logger.warn("Step 3: Final attempt - emergency full extraction");
          const emergencyResult = await shopifyAPI.getAllCustomersFullData(
            executionId
          );

          if (
            emergencyResult.blobResult &&
            emergencyResult.blobResult.success
          ) {
            results.step3.blobResult = emergencyResult.blobResult;
            results.step3.extractionMethod = "emergency-full";
            results.step3.fallbackReason =
              "All primary extraction methods failed";
            results.downloadUrl = emergencyResult.blobResult.blobUrl;

            logger.warn("Step 3: Emergency full extraction successful", {
              blobPath: emergencyResult.blobResult.blobPath,
              blobUrl: emergencyResult.blobResult.blobUrl,
              fields: "CUSTOMER_NODE_FIELDS",
            });
          } else {
            throw new Error("Emergency full extraction also failed");
          }
        } catch (emergencyError) {
          logger.error("Step 3: Emergency extraction failed", {
            error: emergencyError.message,
          });
          throw new Error(
            "Complete extraction failure - all methods including emergency extraction failed"
          );
        }
      }
    } else {
      logger.info("Step 3 skipped: Extraction disabled");
      results.downloadUrl = results.step1.blobResult?.blobUrl;
    }

    if (results.step3.blobResult && results.step3.blobResult.success) {
      results.step4.azureBlobFile = results.step3.blobResult;
      logger.info("Step 4: Using Step 3 blob as final output", {
        blobPath: results.step3.blobResult.blobPath,
        blobUrl: results.step3.blobResult.blobUrl,
        extractionMethod: results.step3.extractionMethod,
        fields: "CUSTOMER_NODE_FIELDS",
      });
    } else {
      logger.error(
        "Step 4: Step 3 failed to create valid blob with customer data"
      );

      if (selectedApproach === "tag-based") {
        try {
          logger.warn(
            "Step 4: Attempting emergency full extraction as last resort"
          );
          const emergencyResult = await shopifyAPI.getAllCustomersFullData(
            executionId
          );

          if (
            emergencyResult.blobResult &&
            emergencyResult.blobResult.success
          ) {
            results.step4.azureBlobFile = emergencyResult.blobResult;
            results.downloadUrl = emergencyResult.blobResult.blobUrl;
            results.step4.emergencyFallback = true;

            logger.warn("Step 4: Emergency full extraction successful", {
              blobPath: emergencyResult.blobResult.blobPath,
              blobUrl: emergencyResult.blobResult.blobUrl,
              fields: "CUSTOMER_NODE_FIELDS",
              method: "emergency-full-extraction",
            });
          } else {
            throw new Error("Emergency full extraction also failed");
          }
        } catch (emergencyError) {
          logger.error("Step 4: Emergency extraction failed", {
            error: emergencyError.message,
          });
          throw new Error(
            "Complete extraction failure - Step 3 failed and emergency extraction failed"
          );
        }
      } else {
        throw new Error("No valid blob available from Step 3 extraction");
      }
    }
    // Step 4.5: Tag Removal
    if (
      removeTagsAfterUpload &&
      selectedApproach === "tag-based" &&
      results.step4.azureBlobFile?.success &&
      results.step1.customerIds.length > 0
    ) {
      try {
        logger.info(
          "Step 4.5: Starting parallel tag removal after successful upload"
        );
        const tagRemovalResults = await shopifyAPI.bulkRemoveCustomerTags(
          results.step1.customerIds,
          customTag
        );

        results.step4_5 = {
          tagRemovalResults,
          cleanupCompleted: true,
        };

        const removalSuccessRate =
          tagRemovalResults.successful + tagRemovalResults.failed > 0
            ? (
                (tagRemovalResults.successful /
                  (tagRemovalResults.successful + tagRemovalResults.failed)) *
                100
              ).toFixed(1)
            : "0";

        logger.info("Step 4.5 completed: Tag removal finished", {
          totalAttempted: results.step1.customerIds.length,
          successful: tagRemovalResults.successful,
          failed: tagRemovalResults.failed,
          skipped: tagRemovalResults.skipped,
          successRate: removalSuccessRate + "%",
        });

        if (tagRemovalResults.failed > 0) {
          results.errors.push({
            step: 4.5,
            error: `Tag removal partially failed: ${tagRemovalResults.failed}/${results.step1.customerIds.length} customers still have the tag`,
            severity: "warning",
          });
        }
      } catch (error) {
        logger.error("Step 4.5 failed", { error: error.message });
        results.errors.push({
          step: 4.5,
          error: "Tag removal failed: " + error.message,
          severity: "warning",
        });
        results.step4_5 = {
          tagRemovalResults: null,
          cleanupCompleted: false,
          error: error.message,
        };
      }
    } else {
      logger.info("Step 4.5 skipped: Tag removal not applicable or disabled");
    }

    // Step 5: Validation (skipped since data is not in memory)
    if (
      validateData &&
      results.step1.totalCount > 0 &&
      selectedApproach === "tag-based"
    ) {
      try {
        logger.info("Step 5: Validating data consistency");
        const additionalInfo =
          selectedApproach === "date-based" ? { startDate, endDate } : {};
        const dataValidation = shopifyAPI.validateDataConsistency(
          results.step1.totalCount,
          0, // Cannot validate extracted count since data is streamed
          selectedApproach,
          additionalInfo
        );
        results.step5.dataValidation = dataValidation;

        logger.info("Step 5: Data validation skipped due to streaming", {
          message:
            "Cannot validate extracted count as data is streamed directly to blob",
        });
      } catch (error) {
        logger.error("Step 5 failed", { error: error.message });
        results.errors.push({ step: 5, error: error.message });
      }
    } else {
      logger.info("Step 5 skipped: Data validation disabled or not applicable");
    }

    const endTime = new Date();
    results.endTime = endTime.toISOString();
    results.duration = endTime.getTime() - startTime.getTime();
    results.success =
      results.errors.filter((e) => e.severity !== "warning").length === 0;

    logger.azureInfo("Extraction completed", {
      executionId,
      approach: selectedApproach,
      duration: Math.round(results.duration / 1000) + "s",
      success: results.success,
      logFile: logger.logFilePath,
      downloadUrl: results.downloadUrl || "none",
      finalBlobPath: results.step4.azureBlobFile?.blobPath || "none",
      finalBlobFields:
        results.step4.azureBlobFile === results.step3.blobResult
          ? "CUSTOMER_NODE_FIELDS"
          : "MINIMAL_NODE_FIELDS",
    });

    response = {
      status: results.success ? 200 : 207,
      headers: { "Content-Type": "application/json" },
      body: results,
    };
  } catch (error) {
    logger.error("Extraction failed", {
      executionId,
      error: error.message,
      stack: error.stack,
    });
    response = {
      status: 500,
      headers: { "Content-Type": "application/json" },
      body: {
        executionId,
        success: false,
        error: error.message,
        timestamp: new Date().toISOString(),
        duration: new Date().getTime() - startTime.getTime(),
        logFile: logger.logFilePath,
      },
    };
  }

  try {
    if (typeof response.body !== "string") {
      response.body = JSON.stringify(response.body);
    }
  } catch (serializeError) {
    logger.azureError("Failed to serialize response body", {
      error: serializeError.message,
    });
    response.body = JSON.stringify({
      executionId,
      success: false,
      error: "Failed to serialize response",
      timestamp: new Date().toISOString(),
      logFile: logger.logFilePath,
    });
  }
  return response;
}

class ShopifyBulkAPI {
  constructor() {
    const cfg = config;

    if (!cfg) {
      throw new Error(
        "Configuration must be loaded before creating ShopifyBulkAPI instance"
      );
    }

    this.shopDomain = cfg.SHOPIFY_STORE_URL;
    this.accessToken = cfg.SHOPIFY_ACCESS_TOKEN;
    this.apiVersion = cfg.SHOPIFY_API_VERSION;
    this.baseURL =
      "https://" + this.shopDomain + "/admin/api/" + this.apiVersion;
    this.maxRetries = parseInt(cfg.MAX_RETRY_ATTEMPTS);
    this.retryDelay = parseInt(cfg.RETRY_DELAY_MS);
    this.pollInterval = 5000; // Increased from 3000ms
    this.maxPollAttempts = 1200; // Increased from 600
    this.maxPollDuration = 3600000; // 60 minutes (from 30 minutes)
    this.maxConcurrentBatches = parseInt(cfg.MAX_CONCURRENT_BATCHES);
    this.downloadTimeout = 1800000; // 30 minutes (from 10 minutes)
    this.requestTimeout = 600000; // 10 minutes (from 5 minutes)

    console.log("maxConcurrentBatches", this.maxConcurrentBatches);

    if (!this.shopDomain || !this.accessToken) {
      throw new Error("Missing required Shopify credentials in configuration");
    }

    console.log("ShopifyBulkAPI initialized", {
      shopDomain: this.shopDomain,
      apiVersion: this.apiVersion,
    });
  }

  async makeRequest(endpoint, method = "GET", data = null, retryCount = 0) {
    try {
      const requestConfig = {
        method,
        url: this.baseURL + endpoint,
        headers: {
          "X-Shopify-Access-Token": this.accessToken,
          "Content-Type": "application/json",
          Accept: "application/json",
        },
        timeout: this.requestTimeout,
        validateStatus: (status) => status >= 200 && status < 300,
      };

      if (data) {
        requestConfig.data = data;

        // ENHANCED: Log request data for GraphQL debugging
        if (endpoint.includes("graphql") && data.query) {
          logger.debug("GraphQL request details", {
            endpoint,
            queryLength: data.query.length,
            queryStart: data.query.substring(0, 100),
            retryCount,
          });
        }
      }

      logger.debug("Making " + method + " request to " + endpoint, {
        retryCount,
      });

      const response = await axios(requestConfig);

      // ENHANCED: Log GraphQL response structure for debugging
      if (endpoint.includes("graphql")) {
        logger.debug("GraphQL response structure", {
          hasData: !!response.data?.data,
          hasErrors: !!response.data?.errors,
          errorCount: response.data?.errors?.length || 0,
          dataKeys: response.data?.data ? Object.keys(response.data.data) : [],
        });

        // Log any GraphQL errors
        if (response.data?.errors) {
          logger.warn("GraphQL response contains errors", {
            errors: response.data.errors,
          });
        }
      }

      if (response.headers["x-shopify-shop-api-call-limit"]) {
        logger.debug("API call limit", {
          limit: response.headers["x-shopify-shop-api-call-limit"],
        });
      }

      return response.data;
    } catch (error) {
      const statusCode = error.response?.status;
      const errorMessage = error.response?.data?.errors || error.message;
      const responseData = error.response?.data;

      logger.error("Request failed", {
        endpoint,
        method,
        status: statusCode,
        error: errorMessage,
        retryCount,
        responseData: responseData
          ? JSON.stringify(responseData, null, 2)
          : null,
      });

      if (statusCode === 403) {
        throw new Error(
          "Access forbidden (403): Check API permissions for " + endpoint
        );
      }

      if (statusCode === 401) {
        throw new Error(
          "Unauthorized (401): Check Shopify access token validity"
        );
      }

      if (statusCode === 400) {
        logger.error("Bad Request (400) - detailed error info", {
          responseData: responseData,
          requestData: data,
          endpoint,
        });
        throw new Error(
          "Bad Request (400): " + (errorMessage || "Invalid request format")
        );
      }

      if (
        (statusCode === 429 || statusCode >= 500) &&
        retryCount < this.maxRetries
      ) {
        // Exponential backoff for retry delays
        const exponentialDelay =
          statusCode === 429
            ? Math.min(5000 * Math.pow(2, retryCount), maxFallBackTime)
            : Math.min(
                this.retryDelay * Math.pow(2, retryCount),
                maxFallBackTime
              );

        logger.warn("Retrying request with exponential backoff", {
          endpoint,
          retryCount: retryCount + 1,
          baseDelay: statusCode === 429 ? 5000 : this.retryDelay,
          exponentialDelay,
          delayMs: exponentialDelay + "ms",
        });

        await this.delay(exponentialDelay);
        return this.makeRequest(endpoint, method, data, retryCount + 1);
      }

      throw error;
    }
  }

  delay(ms) {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }

  async getCurrentBulkOperation() {
    const query = `
      query {
        currentBulkOperation {
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
    `;

    try {
      const response = await this.makeRequest("/graphql.json", "POST", {
        query,
      });
      return response.data?.currentBulkOperation;
    } catch (error) {
      logger.warn("Failed to get current bulk operation", {
        error: error.message,
      });
      return null;
    }
  }

  async getCustomersWithoutTag(tag, executionId) {
    const extractionTag = tag || config.EXTRACTION_TAG || "extracted-bulk";
    logger.info(
      "Getting customers WITHOUT tag using negative query: " + extractionTag,
      { executionId }
    );

    const query = `
    {
      customers(query: "-tag:${extractionTag}") {
        edges {
          node {
            id
            legacyResourceId
            tags
          }
        }
      }
    }
  `;

    const bulkOperation = await this.createBulkOperation(query);
    const completedOperation = await this.waitForBulkOperation(
      bulkOperation.id
    );

    if (!completedOperation.url) {
      logger.warn(
        "Bulk operation completed but no download URL provided for negative tag query",
        { executionId }
      );
      return [];
    }

    const blobResult = await this.downloadBulkDataWithFallback(
      completedOperation.url,
      extractionTag,
      "tag-based",
      executionId
    );

    logger.info(
      "Successfully streamed untagged customer IDs to Azure Blob Storage",
      {
        blobPath: blobResult.blobPath,
        blobUrl: blobResult.blobUrl,
        sizeMB: Math.round(blobResult.sizeBytes / (1024 * 1024)),
        executionId,
      }
    );

    return []; // Return empty array since data is streamed to blob
  }

  async cancelCurrentBulkOperation() {
    logger.info("Attempting to cancel current bulk operation");

    const currentOp = await this.getCurrentBulkOperation();
    if (!currentOp) {
      logger.info("No bulk operation found to cancel");
      return null;
    }

    if (
      currentOp.status === "COMPLETED" ||
      currentOp.status === "FAILED" ||
      currentOp.status === "CANCELED"
    ) {
      logger.info("Current bulk operation is already in terminal state", {
        operationId: currentOp.id,
        status: currentOp.status,
      });
      return currentOp;
    }

    logger.info("Found active bulk operation to cancel", {
      operationId: currentOp.id,
      status: currentOp.status,
      createdAt: currentOp.createdAt,
    });

    const mutation = `
      mutation {
        bulkOperationCancel {
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

    try {
      const response = await this.makeRequest("/graphql.json", "POST", {
        query: mutation,
      });

      if (response.data?.bulkOperationCancel?.userErrors?.length > 0) {
        const errors = response.data.bulkOperationCancel.userErrors;
        logger.warn("Failed to cancel bulk operation", { errors });
        return null;
      }

      const operation = response.data?.bulkOperationCancel?.bulkOperation;
      if (operation) {
        logger.info("Bulk operation cancelled successfully", {
          operationId: operation.id,
          status: operation.status,
        });

        logger.info("Waiting for cancellation to propagate...");
        await this.delay(3000);

        const verifyOp = await this.getCurrentBulkOperation();
        if (
          verifyOp &&
          (verifyOp.status === "CANCELED" || verifyOp.status === "FAILED")
        ) {
          logger.info("Cancellation verified", {
            operationId: verifyOp.id,
            status: verifyOp.status,
          });
        } else {
          logger.warn("Cancellation may not have completed yet", {
            operationId: verifyOp?.id,
            status: verifyOp?.status,
          });
        }
      }

      return operation;
    } catch (error) {
      logger.error("Error canceling bulk operation", { error: error.message });
      return null;
    }
  }

  async createBulkOperation(query) {
    logger.info("Creating bulk operation with existing operation check", {
      queryLength: query.length,
    });

    try {
      const currentOperation = await this.getCurrentBulkOperation();

      if (currentOperation) {
        logger.info("Found existing bulk operation", {
          operationId: currentOperation.id,
          status: currentOperation.status,
          createdAt: currentOperation.createdAt,
        });

        if (currentOperation.status === "COMPLETED") {
          logger.info(
            "Existing bulk operation is completed, will create new one"
          );
        } else if (
          currentOperation.status === "RUNNING" ||
          currentOperation.status === "CREATED"
        ) {
          logger.info(
            "Existing bulk operation is running, waiting for completion"
          );
          return await this.waitForBulkOperation(currentOperation.id);
        } else if (
          currentOperation.status === "FAILED" ||
          currentOperation.status === "CANCELED"
        ) {
          logger.info(
            "Existing bulk operation failed/canceled, creating new one"
          );
          await this.delay(2000);
        }
      }
    } catch (error) {
      logger.warn("Failed to check existing bulk operation", {
        error: error.message,
      });
    }

    // FIXED: Escape quotes properly in the GraphQL query
    const escapedQuery = query.replace(/"/g, '\\"');

    const mutation = `
    mutation {
      bulkOperationRunQuery(
        query: "${escapedQuery}"
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

    try {
      logger.info("Sending bulk operation creation request...");

      // ENHANCED: Log the actual request being sent for debugging
      logger.debug("GraphQL mutation details", {
        mutationLength: mutation.length,
        queryLength: query.length,
        escapedQueryLength: escapedQuery.length,
        endpoint: "/graphql.json",
      });

      const response = await this.makeRequest("/graphql.json", "POST", {
        query: mutation,
      });

      // ENHANCED: Log the full response for debugging
      logger.debug("Bulk operation creation response received", {
        hasData: !!response.data,
        hasBulkOperationRunQuery: !!response.data?.bulkOperationRunQuery,
        hasBulkOperation: !!response.data?.bulkOperationRunQuery?.bulkOperation,
        hasUserErrors: !!response.data?.bulkOperationRunQuery?.userErrors,
        userErrorsLength:
          response.data?.bulkOperationRunQuery?.userErrors?.length || 0,
        responseKeys: response.data ? Object.keys(response.data) : [],
      });

      // ENHANCED: Check for GraphQL errors in the response
      if (response.errors && response.errors.length > 0) {
        logger.error("GraphQL errors in bulk operation creation", {
          errors: response.errors,
        });
        throw new Error(
          "GraphQL errors: " + response.errors.map((e) => e.message).join(", ")
        );
      }

      // ENHANCED: Check if the response structure is what we expect
      if (!response.data) {
        logger.error("No data field in response", {
          response: JSON.stringify(response, null, 2),
        });
        throw new Error("Invalid GraphQL response: missing data field");
      }

      if (!response.data.bulkOperationRunQuery) {
        logger.error("No bulkOperationRunQuery field in response.data", {
          dataKeys: Object.keys(response.data),
          responseData: JSON.stringify(response.data, null, 2),
        });
        throw new Error(
          "Invalid GraphQL response: missing bulkOperationRunQuery field"
        );
      }

      const bulkOpResult = response.data.bulkOperationRunQuery;

      // Check for user errors first
      if (bulkOpResult.userErrors && bulkOpResult.userErrors.length > 0) {
        const errors = bulkOpResult.userErrors;

        logger.error("User errors in bulk operation creation", {
          errors: errors,
        });

        const existingOpError = errors.find(
          (error) =>
            error.message && error.message.includes("already in progress")
        );

        if (existingOpError) {
          logger.warn("Bulk operation already in progress error received", {
            error: existingOpError.message,
          });

          const match = existingOpError.message.match(
            /gid:\/\/shopify\/BulkOperation\/(\d+)/
          );
          if (match) {
            const operationId = "gid://shopify/BulkOperation/" + match[1];
            logger.info(
              "Found existing operation ID in error, waiting for it",
              { operationId }
            );
            return await this.waitForBulkOperation(operationId);
          }

          logger.info(
            'Attempting to find current operation after "already in progress" error'
          );
          const currentOp = await this.getCurrentBulkOperation();
          if (
            currentOp &&
            (currentOp.status === "RUNNING" || currentOp.status === "CREATED")
          ) {
            logger.info("Found running operation, waiting for completion", {
              operationId: currentOp.id,
            });
            return await this.waitForBulkOperation(currentOp.id);
          } else {
            throw new Error(
              "Bulk operation already in progress but cannot find the operation to wait for"
            );
          }
        }

        throw new Error(
          "Bulk operation creation failed: " + JSON.stringify(errors)
        );
      }

      // ENHANCED: Check for bulk operation with better error messages
      if (!bulkOpResult.bulkOperation) {
        logger.error("No bulkOperation field in bulkOperationRunQuery", {
          bulkOpResultKeys: Object.keys(bulkOpResult),
          bulkOpResult: JSON.stringify(bulkOpResult, null, 2),
          hasUserErrors: !!bulkOpResult.userErrors,
          userErrors: bulkOpResult.userErrors,
        });

        // Try to provide more specific error information
        if (bulkOpResult.userErrors && bulkOpResult.userErrors.length === 0) {
          throw new Error(
            "Bulk operation creation returned no errors but also no operation. This may indicate a permissions issue or API version incompatibility."
          );
        } else {
          throw new Error(
            "Invalid response from bulk operation creation: missing bulkOperation field"
          );
        }
      }

      const operation = bulkOpResult.bulkOperation;

      logger.info("New bulk operation created successfully", {
        operationId: operation.id,
        status: operation.status,
        createdAt: operation.createdAt,
      });

      return operation;
    } catch (error) {
      // ENHANCED: Better error handling and logging
      logger.error("Bulk operation creation failed", {
        error: error.message,
        stack: error.stack,
        queryLength: query.length,
        mutationLength: mutation.length,
      });

      if (error.message && error.message.includes("already in progress")) {
        logger.info(
          "Bulk operation already in progress, attempting to find and wait for it"
        );

        try {
          const currentOp = await this.getCurrentBulkOperation();
          if (
            currentOp &&
            (currentOp.status === "RUNNING" || currentOp.status === "CREATED")
          ) {
            logger.info("Found running operation, waiting for completion", {
              operationId: currentOp.id,
            });
            return await this.waitForBulkOperation(currentOp.id);
          } else {
            throw new Error(
              "Bulk operation already in progress but no running operation found"
            );
          }
        } catch (getCurrentError) {
          logger.error(
            'Failed to get current operation after "already in progress" error',
            {
              error: getCurrentError.message,
            }
          );
          throw new Error("Failed to handle existing bulk operation");
        }
      }

      // Re-throw the original error with additional context
      throw new Error(`Bulk operation creation failed: ${error.message}`);
    }
  }

  async getBulkOperationStatus(operationId) {
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
            partialDataUrl
          }
        }
      }
    `;

    const response = await this.makeRequest("/graphql.json", "POST", { query });
    return response.data?.node;
  }

  async waitForBulkOperation(operationId) {
    logger.info("Waiting for bulk operation", { operationId });
    const startTime = Date.now();
    let attempts = 0;

    while (true) {
      try {
        const operation = await this.getBulkOperationStatus(operationId);

        if (!operation) {
          throw new Error("Failed to get bulk operation status");
        }

        logger.info("Bulk operation status", {
          operationId,
          status: operation.status,
          objectCount: operation.objectCount,
          attempt: attempts + 1,
        });

        if (operation.status === "COMPLETED") {
          logger.info("Bulk operation completed successfully", {
            operationId,
            objectCount: operation.objectCount,
            fileSize: operation.fileSize,
          });
          return operation;
        }

        if (operation.status === "FAILED" || operation.status === "CANCELED") {
          throw new Error(
            "Bulk operation " +
              operation.status.toLowerCase() +
              ": " +
              (operation.errorCode || "Unknown error")
          );
        }
      } catch (error) {
        logger.warn("Error checking bulk operation status", {
          operationId,
          error: error.message,
          attempt: attempts + 1,
        });

        if (Date.now() - startTime > this.maxPollDuration) {
          throw new Error(
            "Bulk operation timed out after " + attempts + " attempts"
          );
        }
      }

      // Exponential backoff: start with pollInterval, double each time, cap at 30 seconds
      const exponentialDelay = Math.min(
        this.pollInterval * Math.pow(1.5, Math.min(attempts, 10)),
        maxFallBackTime
      );

      logger.debug("Using exponential backoff delay", {
        attempt: attempts + 1,
        baseInterval: this.pollInterval,
        exponentialDelay,
        nextCheckIn: exponentialDelay + "ms",
      });

      await this.delay(exponentialDelay);
      attempts++;
    }
  }

  async downloadBulkDataWithFallback(
    url,
    tag,
    approach,
    executionId,
    additionalInfo = {}
  ) {
    logger.info("Starting bulk data streaming directly to Azure Blob Storage", {
      executionId,
      approach,
    });

    // Initialize Azure Blob Service Client
    const blobServiceClient = BlobServiceClient.fromConnectionString(
      config.AZURE_STORAGE_CONNECTION_STRING
    );
    const containerName = config.BLOB_CONTAINER_NAME;
    const containerClient = blobServiceClient.getContainerClient(containerName);

    // Extract store name from shopDomain
    const storeName = this.shopDomain
      .replace(/\.myshopify\.com$/i, "")
      .replace(/[^a-z0-9]/gi, "_");

    // Create folder structure: storename/customers/executionId/
    const folderPath = `${storeName}/customers/${executionId}/`;

    // Generate unique file name: storename_customers_executionId_timestamp.jsonl
    const timestamp = new Date().toISOString().replace(/[:.]/g, "-");
    const fileName = `${storeName}_customers_${executionId}_${timestamp}_${approach}.jsonl`;
    const blobPath = folderPath + fileName;

    const blobClient = containerClient.getBlockBlobClient(blobPath);
    let existingFileFound = false;

    try {
      const blobProperties = await blobClient.getProperties();
      existingFileFound = true;
      logger.info("Existing file found, deleting before stream upload...", {
        blobPath,
      });
      await blobClient.delete({ deleteSnapshots: "include" });
      logger.info("Successfully deleted existing file", { blobPath });
    } catch (error) {
      if (error.statusCode === 404) {
        logger.info("No existing file found - proceeding with stream upload", {
          blobPath,
        });
      } else {
        logger.error("Error checking existing blob", {
          error: error.message,
          blobPath,
        });
      }
    }

    logger.info("Starting streaming download and upload", {
      url: url.substring(0, 100) + "...",
      blobPath,
      containerName,
      storeName,
      fileName,
      executionId,
    });

    try {
      const response = await axios({
        method: "GET",
        url: url,
        responseType: "stream",
        timeout: this.downloadTimeout,
        headers: {
          "User-Agent": "Shopify-Bulk-Extractor/1.0",
          Accept: "text/plain, */*",
          Connection: "keep-alive",
        },
        maxRedirects: 5,
      });

      // Set blob metadata
      const metadata = {
        storeName: storeName,
        approach: approach || "unknown",
        executionId,
        extractionTag: tag || "extracted-bulk",
        extractedAt: new Date().toISOString(),
        fileType: "shopify-customers",
        replacedExisting: existingFileFound.toString(),
        uploadVersion: "1.0",
        uploadMethod: "streaming",
      };

      if (
        approach === "date-based" &&
        additionalInfo.startDate &&
        additionalInfo.endDate
      ) {
        metadata.startDate = additionalInfo.startDate;
        metadata.endDate = additionalInfo.endDate;
        metadata.dateFiltered = "true";
      }

      const uploadOptions = {
        metadata,
        blobHTTPHeaders: {
          blobContentType: "application/x-jsonlines",
        },
        bufferSize: 8 * 1024 * 1024, // 8MB buffer
        maxBuffers: 5, // Max 5 buffers in memory
      };

      // Stream directly to Azure Blob Storage
      const uploadStartTime = Date.now();
      let totalBytes = 0;

      // Temporary buffer to sample first few lines for debugging
      let sampleData = "";
      let lineCount = 0;
      const maxSampleLines = 3;

      response.data.on("data", (chunk) => {
        totalBytes += chunk.length;
        if (lineCount < maxSampleLines) {
          sampleData += chunk.toString();
          const lines = sampleData.split("\n");
          if (lines.length > maxSampleLines) {
            sampleData = lines.slice(0, maxSampleLines).join("\n");
            lineCount = maxSampleLines;
          }
        }
      });

      const uploadResponse = await blobClient.uploadStream(
        response.data,
        uploadOptions.bufferSize,
        uploadOptions.maxBuffers,
        {
          metadata: uploadOptions.metadata,
          blobHTTPHeaders: uploadOptions.blobHTTPHeaders,
        }
      );

      const uploadDuration = Date.now() - uploadStartTime;
      const blobUrl = blobClient.url;

      // Log sample data for debugging
      logger.debug("Sample of streamed data", {
        sampleData: sampleData.split("\n").map((line) => {
          try {
            const parsed = JSON.parse(line);
            return {
              fields: Object.keys(parsed),
              sampleValues: Object.fromEntries(
                Object.entries(parsed).slice(0, 3) // Limit to first 3 fields for brevity
              ),
            };
          } catch (e) {
            return { error: "Failed to parse line: " + e.message };
          }
        }),
        lineCount,
        blobPath,
        executionId,
      });

      logger.info("Successfully streamed data to Azure Blob Storage", {
        blobPath,
        blobUrl,
        requestId: uploadResponse.requestId,
        etag: uploadResponse.etag,
        lastModified: uploadResponse.lastModified,
        sizeBytes: totalBytes,
        sizeMB: Math.round(totalBytes / (1024 * 1024)),
        uploadDurationMs: uploadDuration,
        uploadDurationMinutes: Math.round((uploadDuration / 60000) * 100) / 100,
        containerName,
        fileName,
        replacedExisting: existingFileFound,
        operation: existingFileFound ? "replaced" : "created",
        uploadMethod: "streaming",
        executionId,
      });

      return {
        success: true,
        blobPath,
        blobUrl,
        fileName,
        folderPath,
        storeName,
        containerName,
        sizeBytes: totalBytes,
        approach: approach || "unknown",
        replacedExisting: existingFileFound,
        operation: existingFileFound ? "replaced" : "created",
        uploadDurationMs: uploadDuration,
        uploadResponse: {
          requestId: uploadResponse.requestId,
          etag: uploadResponse.etag,
          lastModified: uploadResponse.lastModified,
        },
        metadata,
      };
    } catch (error) {
      logger.error("Failed to stream data to Azure Blob Storage", {
        error: error.message,
        stack: error.stack,
        url: url.substring(0, 100) + "...",
        containerName,
        blobPath,
        executionId,
      });
      throw new Error(
        "Failed to stream bulk data to Azure Blob Storage: " + error.message
      );
    }
  }

  async extractCustomersByNegateTag(tag, executionId) {
    const extractionTag = tag || config.EXTRACTION_TAG || "extracted-bulk";
    logger.info(
      "Starting customer extraction WITHOUT tag (negate): " + extractionTag,
      { executionId }
    );

    const query = `
    {
      customers(query: "NOT tag:${extractionTag}") {
        edges {
          node {${CUSTOMER_NODE_FIELDS}
          }
        }
      }
    }
  `;

    const bulkOperation = await this.createBulkOperation(query);
    const completedOperation = await this.waitForBulkOperation(
      bulkOperation.id
    );

    if (!completedOperation.url) {
      logger.warn(
        "Bulk operation completed but no download URL provided for negate tag extraction",
        { executionId }
      );
      return { blobResult: null };
    }

    const blobResult = await this.downloadBulkDataWithFallback(
      completedOperation.url,
      extractionTag,
      "tag-based",
      executionId
    );

    logger.info(
      "Successfully streamed untagged customer data to Azure Blob Storage",
      {
        blobPath: blobResult.blobPath,
        blobUrl: blobResult.blobUrl,
        sizeMB: Math.round(blobResult.sizeBytes / (1024 * 1024)),
        executionId,
      }
    );

    return { blobResult };
  }

  async bulkRemoveCustomerTags(customerIds, tag) {
    const extractionTag = tag || config.EXTRACTION_TAG || "extracted-bulk";
    logger.info(
      "Starting parallel bulk tag removal for " +
        customerIds.length +
        " customers with tag: " +
        extractionTag
    );

    if (customerIds.length === 0) {
      return {
        successful: 0,
        failed: 0,
        errors: [],
        skipped: 0,
        removedTags: false,
      };
    }

    const customersToRemoveTags = customerIds;

    logger.info("Processing tag removal for all customers:", {
      totalCustomers: customerIds.length,
      processing: customersToRemoveTags.length,
      tag: extractionTag,
    });

    if (customersToRemoveTags.length === 0) {
      logger.info("No customers need tag removal");
      return {
        successful: customerIds.length,
        failed: 0,
        errors: [],
        skipped: customerIds.length,
        removedTags: false,
      };
    }

    const batchSize = 50;
    const maxConcurrentBatches = this.maxConcurrentBatches;
    const batches = [];

    for (let i = 0; i < customersToRemoveTags.length; i += batchSize) {
      batches.push(customersToRemoveTags.slice(i, i + batchSize));
    }

    const results = {
      successful: 0,
      failed: 0,
      errors: [],
      skipped: 0,
      removedTags: true,
    };

    if (batches.length === 0) {
      logger.info("No customers need tag removal");
      return results;
    }

    logger.info(
      "Processing " +
        batches.length +
        " removal batches of " +
        batchSize +
        " customers each with " +
        this.maxConcurrentBatches +
        " concurrent batches"
    );

    await this.processTagRemovalBatchesInParallel(
      batches,
      extractionTag,
      results,
      maxConcurrentBatches
    );

    logger.info("Parallel bulk tag removal completed", {
      totalProcessed: customerIds.length,
      successful: results.successful,
      failed: results.failed,
      skipped: results.skipped,
      tagsRemoved: results.successful,
    });

    return results;
  }

  async processTagRemovalBatchesInParallel(
    batches,
    extractionTag,
    results,
    maxConcurrent
  ) {
    logger.info("Starting optimized rate-limited tag removal batch processing");

    // Use similar rate limiting as tag addition
    const maxApiCallsPerMinute = 60;
    const baseDelayBetweenChunks = Math.max(
      1000,
      (60000 / maxApiCallsPerMinute) * maxConcurrent
    );
    let currentDelay = Math.max(1000, baseDelayBetweenChunks * 0.5);
    let consecutiveSuccessChunks = 0;

    const totalEstimatedTime = Math.round(
      (batches.length * currentDelay) / 60000
    );

    logger.info("Tag removal rate limiting configuration", {
      maxApiCallsPerMinute,
      maxConcurrentBatches: maxConcurrent,
      startingDelayBetweenChunks: currentDelay + "ms",
      baseDelay: baseDelayBetweenChunks + "ms",
      totalBatches: batches.length,
      estimatedTotalTimeMinutes: totalEstimatedTime,
      strategy: "Aggressive start with adaptive slowdown for tag removal",
    });

    let totalProcessedBatches = 0;
    let lastProgressLog = Date.now();

    for (let i = 0; i < batches.length; i += maxConcurrent) {
      const batchChunk = batches.slice(i, i + maxConcurrent);
      const chunkNumber = Math.floor(i / maxConcurrent) + 1;
      const totalChunks = Math.ceil(batches.length / maxConcurrent);

      const progressPercentage = (
        ((chunkNumber - 1) / totalChunks) *
        100
      ).toFixed(1);
      const remainingChunks = totalChunks - chunkNumber;
      const estimatedTimeRemaining = Math.round(
        (remainingChunks * currentDelay) / 60000
      );

      logger.info(
        `Processing tag removal chunk ${chunkNumber}/${totalChunks} (${progressPercentage}%)`,
        {
          batchesInChunk: batchChunk.length,
          totalCustomersInChunk: batchChunk.reduce(
            (sum, batch) => sum + batch.length,
            0
          ),
          estimatedTimeRemainingMinutes: estimatedTimeRemaining,
          currentDelayMs: currentDelay,
          operation: "tag_removal",
        }
      );

      const batchPromises = batchChunk.map((batch, batchIndex) =>
        this.processSingleTagRemovalBatch(
          batch,
          extractionTag,
          i + batchIndex + 1,
          batches.length
        )
      );

      try {
        const chunkStartTime = Date.now();
        const batchResults = await Promise.all(batchPromises);
        const chunkDuration = Date.now() - chunkStartTime;

        let chunkSuccessful = 0;
        let chunkFailed = 0;

        batchResults.forEach((batchResult) => {
          results.successful += batchResult.successful;
          results.failed += batchResult.failed;
          results.errors.push(...batchResult.errors);
          chunkSuccessful += batchResult.successful;
          chunkFailed += batchResult.failed;
        });

        totalProcessedBatches += batchChunk.length;
        const chunkSuccessRate =
          chunkSuccessful + chunkFailed > 0
            ? (
                (chunkSuccessful / (chunkSuccessful + chunkFailed)) *
                100
              ).toFixed(1)
            : "0";
        const overallSuccessRate =
          results.successful + results.failed > 0
            ? (
                (results.successful / (results.successful + results.failed)) *
                100
              ).toFixed(1)
            : "0";

        // Adaptive speed optimization for tag removal
        if (chunkFailed === 0) {
          consecutiveSuccessChunks++;

          if (consecutiveSuccessChunks >= 10 && currentDelay > 500) {
            const oldDelay = currentDelay;
            currentDelay = Math.max(minimumRetryDelay, currentDelay * 0.93); // 7% speed up

            logger.info(
              "Perfect tag removal success - speeding up processing",
              {
                consecutiveSuccessChunks,
                oldDelay: oldDelay + "ms",
                newDelay: currentDelay + "ms",
                optimization: "increasing_speed",
              }
            );
          }
        } else if (chunkFailed > 0) {
          consecutiveSuccessChunks = 0;
          const oldDelay = currentDelay;
          currentDelay = Math.min(baseDelayBetweenChunks, currentDelay * 1.5);

          logger.warn(
            "Tag removal failures detected - slowing down for stability",
            {
              chunkFailed,
              chunkSuccessRate: chunkSuccessRate + "%",
              oldDelay: oldDelay + "ms",
              newDelay: currentDelay + "ms",
              optimization: "increasing_stability",
            }
          );
        }

        logger.info(
          `Completed tag removal chunk ${chunkNumber}/${totalChunks}`,
          {
            chunkBatches: batchChunk.length,
            chunkSuccessful,
            chunkFailed,
            chunkSuccessRate: chunkSuccessRate + "%",
            chunkDurationMs: chunkDuration,
            totalSuccessful: results.successful,
            totalFailed: results.failed,
            overallSuccessRate: overallSuccessRate + "%",
            totalProcessedBatches,
            remainingBatches: batches.length - totalProcessedBatches,
            nextDelayMs: chunkNumber < totalChunks ? currentDelay : 0,
          }
        );

        if (chunkNumber % 25 === 0 || Date.now() - lastProgressLog > 60000) {
          lastProgressLog = Date.now();
          const newEstimatedTimeRemaining = Math.round(
            (remainingChunks * currentDelay) / 60000
          );

          logger.info(`🧹 TAG REMOVAL - Progress update`, {
            processedChunks: chunkNumber,
            totalChunks,
            progressPercentage: progressPercentage + "%",
            tagsRemovedSoFar: results.successful,
            failedRemovalsSoFar: results.failed,
            currentSuccessRate: overallSuccessRate + "%",
            currentSpeed: `${currentDelay}ms delay`,
            estimatedCompletionMinutes: newEstimatedTimeRemaining,
            optimization:
              consecutiveSuccessChunks >= 10 ? "FAST_MODE" : "NORMAL_MODE",
          });
        }

        if (chunkNumber < totalChunks) {
          await this.delay(currentDelay);
        }
      } catch (error) {
        consecutiveSuccessChunks = 0;
        logger.error(`Tag removal chunk ${chunkNumber} failed with exception`, {
          error: error.message,
          chunkNumber,
          totalChunks,
          willSlowDown: true,
        });

        batchChunk.forEach((batch) => {
          results.failed += batch.length;
          batch.forEach((customerId) => {
            results.errors.push({
              customerId,
              error: "Tag removal chunk failed: " + error.message,
              chunk: chunkNumber,
              timestamp: new Date().toISOString(),
            });
          });
        });

        currentDelay = Math.min(baseDelayBetweenChunks * 2, 10000);
        logger.warn(
          `Tag removal exception occurred - slowing down significantly`,
          {
            newDelay: currentDelay + "ms",
            safetyMode: "enabled",
          }
        );

        if (chunkNumber < totalChunks) {
          await this.delay(currentDelay);
        }
      }
    }

    const finalSuccessRate =
      results.successful + results.failed > 0
        ? (
            (results.successful / (results.successful + results.failed)) *
            100
          ).toFixed(1)
        : "0";

    logger.info("🎉 Tag removal processing completed", {
      totalBatchesProcessed: totalProcessedBatches,
      totalTagsRemoved: results.successful,
      totalFailed: results.failed,
      finalSuccessRate: finalSuccessRate + "%",
      strategy: "Adaptive speed optimization for cleanup",
      finalDelay: currentDelay + "ms",
      maxSpeedAchieved: consecutiveSuccessChunks >= 10 ? "YES" : "NO",
      performance:
        finalSuccessRate > 95
          ? "EXCELLENT"
          : finalSuccessRate > 85
          ? "GOOD"
          : "NEEDS_IMPROVEMENT",
    });
  }

  async processSingleTagRemovalBatch(
    batch,
    extractionTag,
    batchNumber,
    totalBatches
  ) {
    const startTime = Date.now();

    try {
      logger.debug(
        "Starting tag removal batch " +
          batchNumber +
          "/" +
          totalBatches +
          " (" +
          batch.length +
          " customers)"
      );

      const mutations = batch
        .map(
          (customerId, index) => `
      customer${index}: tagsRemove(
        id: "gid://shopify/Customer/${customerId}"
        tags: ["${extractionTag}"]
      ) {
        node {
          id
        }
        userErrors {
          field
          message
        }
      }
    `
        )
        .join("\n");

      const mutation = "mutation { " + mutations + " }";

      const response = await this.makeRequest("/graphql.json", "POST", {
        query: mutation,
      });

      const batchResult = { successful: 0, failed: 0, errors: [] };

      batch.forEach((customerId, index) => {
        const customerKey = "customer" + index;
        const result = response.data?.[customerKey];

        if (!result) {
          batchResult.failed++;
          batchResult.errors.push({
            customerId,
            error: "No result returned for " + customerKey + " tag removal",
            batchNumber,
          });
          return;
        }

        if (result.userErrors && result.userErrors.length > 0) {
          batchResult.failed++;
          batchResult.errors.push({
            customerId,
            error: "Tag removal error: " + result.userErrors[0].message,
            batchNumber,
          });
        } else {
          batchResult.successful++;
        }
      });

      const duration = Date.now() - startTime;
      logger.debug(
        "Completed tag removal batch " + batchNumber + "/" + totalBatches,
        {
          batchSize: batch.length,
          successful: batchResult.successful,
          failed: batchResult.failed,
          durationMs: duration,
        }
      );

      return batchResult;
    } catch (error) {
      logger.error("Tag removal batch " + batchNumber + " failed", {
        error: error.message,
        batchSize: batch.length,
        duration: Date.now() - startTime,
      });

      const batchResult = {
        successful: 0,
        failed: batch.length,
        errors: batch.map((customerId) => ({
          customerId,
          error: "Tag removal batch failed: " + error.message,
          batchNumber,
        })),
      };

      return batchResult;
    }
  }

  async getAllCustomerIds(executionId) {
    logger.info("Starting customer ID extraction", { executionId });
    const query = `
    {
      customers {
        edges {
          node {${MINIMAL_NODE_FIELDS}
          }
        }
      }
    }
  `;

    try {
      const bulkOperation = await this.createBulkOperation(query);
      const completedOperation = await this.waitForBulkOperation(
        bulkOperation.id
      );

      if (!completedOperation.url) {
        throw new Error(
          "Bulk operation completed but no download URL provided"
        );
      }

      logger.info("Downloading customer IDs for tagging purposes only");

      const response = await axios({
        method: "GET",
        url: completedOperation.url,
        timeout: this.downloadTimeout,
        headers: {
          "User-Agent": "Shopify-Bulk-Extractor/1.0",
          Accept: "text/plain, */*",
        },
      });

      const customerIds = [];
      const lines = response.data.split("\n").filter((line) => line.trim());

      for (const line of lines) {
        try {
          const customer = JSON.parse(line);
          if (customer.legacyResourceId) {
            customerIds.push(customer.legacyResourceId);
          }
        } catch (parseError) {
          logger.debug("Skipping invalid JSON line", {
            line: line.substring(0, 100),
          });
        }
      }

      logger.info("Successfully extracted customer IDs for tagging", {
        totalCustomers: customerIds.length,
        executionId,
        note: "No blob created in Step 1 - IDs only used for tagging",
      });

      return {
        customerIds, // Return actual IDs for tagging
        customerData: [],
        blobResult: null, // IMPORTANT: No blob in Step 1
      };
    } catch (error) {
      logger.error("Failed to extract customer IDs", {
        error: error.message,
        executionId,
      });
      throw error;
    }
  }

  async getAllCustomersFullData(executionId) {
    logger.info("Starting full customer data extraction (Full Approach)", {
      executionId,
    });
    const query = `
    {
      customers {
        edges {
          node {${CUSTOMER_NODE_FIELDS}
          }
        }
      }
    }
  `;
    const bulkOperation = await this.createBulkOperation(query);
    const completedOperation = await this.waitForBulkOperation(
      bulkOperation.id
    );

    if (!completedOperation.url) {
      throw new Error("Bulk operation completed but no download URL provided");
    }

    const blobResult = await this.downloadBulkDataWithFallback(
      completedOperation.url,
      config.EXTRACTION_TAG || "extracted-bulk",
      "full",
      executionId
    );

    logger.info(
      "Successfully streamed full customer data to Azure Blob Storage",
      {
        blobPath: blobResult.blobPath,
        blobUrl: blobResult.blobUrl,
        sizeMB: Math.round(blobResult.sizeBytes / (1024 * 1024)),
        executionId,
      }
    );

    return { blobResult }; // Return blob metadata
  }

  async getAllCustomersFullDataWithDateFilter(startDate, endDate, executionId) {
    logger.info(
      "Starting full customer data extraction with date filter (Date-based Approach)",
      { startDate, endDate, executionId }
    );

    const start = new Date(startDate).toISOString();
    const end = new Date(endDate).toISOString();
    const dateFilter = `updated_at:>='${start}' AND updated_at:<='${end}'`;

    logger.info("Date filter applied for SERVER-SIDE filtering", {
      startDate: start,
      endDate: end,
      filter: dateFilter,
      dateRangeDays: Math.ceil(
        (new Date(end) - new Date(start)) / (1000 * 60 * 60 * 24)
      ),
      executionId,
    });

    const query = `
    {
      customers(query: "${dateFilter}") {
        edges {
          node {${CUSTOMER_NODE_FIELDS}
          }
        }
      }
    }
  `;

    logger.info("Creating bulk operation with SERVER-SIDE date filter query", {
      queryContains: `customers(query: "${dateFilter}"...)`,
      bulkProcessing:
        "Shopify will only process customers matching the date filter",
      executionId,
    });

    const bulkOperation = await this.createBulkOperation(query);
    const completedOperation = await this.waitForBulkOperation(
      bulkOperation.id
    );

    logger.info("Bulk operation completed with server-side filtering", {
      operationId: completedOperation.id,
      objectCount: completedOperation.objectCount,
      fileSize: completedOperation.fileSize,
      hasUrl: !!completedOperation.url,
      dateFilter,
      executionId,
    });

    if (completedOperation.objectCount === 0 || !completedOperation.url) {
      logger.info("No customers found matching the date filter", {
        dateFilter,
        startDate: start,
        endDate: end,
        objectCount: completedOperation.objectCount,
        executionId,
      });
      return { blobResult: null };
    }

    const blobResult = await this.downloadBulkDataWithFallback(
      completedOperation.url,
      config.EXTRACTION_TAG || "extracted-bulk",
      "date-based",
      executionId,
      { startDate, endDate }
    );

    logger.info(
      "Successfully streamed customer data with date filter to Azure Blob Storage",
      {
        blobPath: blobResult.blobPath,
        blobUrl: blobResult.blobUrl,
        sizeMB: Math.round(blobResult.sizeBytes / (1024 * 1024)),
        startDate,
        endDate,
        dateFilter,
        executionId,
      }
    );

    return { blobResult };
  }

  async bulkTagCustomers(customerIds, tag, existingCustomerData = null) {
    const extractionTag = tag || config.EXTRACTION_TAG || "extracted-bulk";
    logger.info(
      "Starting parallel bulk tagging for " +
        customerIds.length +
        " customers with tag: " +
        extractionTag
    );

    if (customerIds.length === 0) {
      return {
        successful: 0,
        failed: 0,
        errors: [],
        skipped: 0,
        alreadyTagged: false,
      };
    }

    logger.info("Checking for customers who already have the tag...");

    const customersToTag = customerIds;

    logger.info("Processing all customers without tag checks:", {
      totalCustomers: customerIds.length,
      processing: customersToTag.length,
      skippedChecks: true,
    });

    if (customersToTag.length === 0) {
      logger.info(
        "All customers already have the tag, skipping tagging process"
      );
      return {
        successful: customerIds.length,
        failed: 0,
        errors: [],
        skipped: customerIds.length,
        alreadyTagged: true,
      };
    }

    const batchSize = 50;
    const maxConcurrentBatches = this.maxConcurrentBatches;
    const batches = [];

    for (let i = 0; i < customersToTag.length; i += batchSize) {
      batches.push(customersToTag.slice(i, i + batchSize));
    }

    const results = {
      successful: 0,
      failed: 0,
      errors: [],
      skipped: 0,
      alreadyTagged: false,
    };

    if (batches.length === 0) {
      logger.info("No customers need tagging");
      return results;
    }

    logger.info(
      "Processing " +
        batches.length +
        " batches of " +
        batchSize +
        " customers each with " +
        this.maxConcurrentBatches +
        " concurrent batches"
    );

    await this.processTaggingBatchesInParallel(
      batches,
      extractionTag,
      results,
      maxConcurrentBatches
    );

    logger.info("Parallel bulk tagging completed (Fast mode)", {
      totalProcessed: customerIds.length,
      successful: results.successful,
      failed: results.failed,
      skipped: results.skipped,
      newlyTagged: results.successful,
    });

    return results;
  }

  async processTaggingBatchesInParallel(
    batches,
    extractionTag,
    results,
    maxConcurrent
  ) {
    logger.info("Starting OPTIMIZED FAST rate-limited batch processing");

    // OPTIMIZED SHOPIFY RATE LIMITING
    // Since we're getting 100% success rate, we can be more aggressive
    // Shopify GraphQL Admin API: 1,000 points per minute
    // Each tagging mutation: ~10-20 points
    // Optimized approach: 60 API calls per minute = 1 call per second
    const maxApiCallsPerMinute = 60;
    const baseDelayBetweenChunks = Math.max(
      1000,
      (60000 / maxApiCallsPerMinute) * maxConcurrent
    );

    // Start with faster processing, but be ready to slow down if needed
    let currentDelay = Math.max(1000, baseDelayBetweenChunks * 0.5); // Start 50% faster
    let consecutiveSuccessChunks = 0;

    const totalEstimatedTime = Math.round(
      (batches.length * currentDelay) / 60000
    );

    logger.info("Optimized rate limiting configuration", {
      maxApiCallsPerMinute,
      maxConcurrentBatches: maxConcurrent,
      startingDelayBetweenChunks: currentDelay + "ms",
      baseDelay: baseDelayBetweenChunks + "ms",
      totalBatches: batches.length,
      estimatedTotalTimeMinutes: totalEstimatedTime,
      strategy: "Aggressive start with adaptive slowdown",
    });

    let totalProcessedBatches = 0;
    let lastProgressLog = Date.now();

    for (let i = 0; i < batches.length; i += maxConcurrent) {
      const batchChunk = batches.slice(i, i + maxConcurrent);
      const chunkNumber = Math.floor(i / maxConcurrent) + 1;
      const totalChunks = Math.ceil(batches.length / maxConcurrent);

      // Calculate progress and ETA with current delay
      const progressPercentage = (
        ((chunkNumber - 1) / totalChunks) *
        100
      ).toFixed(1);
      const remainingChunks = totalChunks - chunkNumber;
      const estimatedTimeRemaining = Math.round(
        (remainingChunks * currentDelay) / 60000
      );

      logger.info(
        `Processing chunk ${chunkNumber}/${totalChunks} (${progressPercentage}%)`,
        {
          batchesInChunk: batchChunk.length,
          totalCustomersInChunk: batchChunk.reduce(
            (sum, batch) => sum + batch.length,
            0
          ),
          estimatedTimeRemainingMinutes: estimatedTimeRemaining,
          currentDelayMs: currentDelay,
          rateLimitOptimized: true,
        }
      );

      const batchPromises = batchChunk.map((batch, batchIndex) =>
        this.processSingleTaggingBatch(
          batch,
          extractionTag,
          i + batchIndex + 1,
          batches.length
        )
      );

      try {
        const chunkStartTime = Date.now();
        const batchResults = await Promise.all(batchPromises);
        const chunkDuration = Date.now() - chunkStartTime;

        // Track chunk-level statistics
        let chunkSuccessful = 0;
        let chunkFailed = 0;

        batchResults.forEach((batchResult) => {
          results.successful += batchResult.successful;
          results.failed += batchResult.failed;
          results.errors.push(...batchResult.errors);
          chunkSuccessful += batchResult.successful;
          chunkFailed += batchResult.failed;
        });

        totalProcessedBatches += batchChunk.length;
        const chunkSuccessRate =
          chunkSuccessful + chunkFailed > 0
            ? (
                (chunkSuccessful / (chunkSuccessful + chunkFailed)) *
                100
              ).toFixed(1)
            : "0";
        const overallSuccessRate =
          results.successful + results.failed > 0
            ? (
                (results.successful / (results.successful + results.failed)) *
                100
              ).toFixed(1)
            : "0";

        // ADAPTIVE SPEED OPTIMIZATION
        if (chunkFailed === 0) {
          // Perfect success - we can go faster
          consecutiveSuccessChunks++;

          if (consecutiveSuccessChunks >= 10 && currentDelay > 500) {
            const oldDelay = currentDelay;
            currentDelay = Math.max(minimumRetryDelay, currentDelay * 0.93); // Speed up by 7%

            logger.info("Perfect success rate - speeding up processing", {
              consecutiveSuccessChunks,
              oldDelay: oldDelay + "ms",
              newDelay: currentDelay + "ms",
              optimization: "increasing_speed",
            });
          }
        } else if (chunkFailed > 0) {
          // Some failures - slow down to be safe
          consecutiveSuccessChunks = 0;
          const oldDelay = currentDelay;
          currentDelay = Math.min(baseDelayBetweenChunks, currentDelay * 1.5); // Slow down by 50%

          logger.warn("Failures detected - slowing down for stability", {
            chunkFailed,
            chunkSuccessRate: chunkSuccessRate + "%",
            oldDelay: oldDelay + "ms",
            newDelay: currentDelay + "ms",
            optimization: "increasing_stability",
          });
        }

        logger.info(`Completed chunk ${chunkNumber}/${totalChunks}`, {
          chunkBatches: batchChunk.length,
          chunkSuccessful,
          chunkFailed,
          chunkSuccessRate: chunkSuccessRate + "%",
          chunkDurationMs: chunkDuration,
          totalSuccessful: results.successful,
          totalFailed: results.failed,
          overallSuccessRate: overallSuccessRate + "%",
          totalProcessedBatches,
          remainingBatches: batches.length - totalProcessedBatches,
          nextDelayMs: chunkNumber < totalChunks ? currentDelay : 0,
        });

        // PROGRESS LOGGING: More frequent updates for faster processing
        const now = Date.now();
        if (chunkNumber % 25 === 0 || now - lastProgressLog > 60000) {
          // Every 25 chunks or 1 minute
          lastProgressLog = now;
          const newEstimatedTimeRemaining = Math.round(
            (remainingChunks * currentDelay) / 60000
          );

          logger.info(`SPEED OPTIMIZED - Progress update`, {
            processedChunks: chunkNumber,
            totalChunks,
            progressPercentage: progressPercentage + "%",
            successfulSoFar: results.successful,
            failedSoFar: results.failed,
            currentSuccessRate: overallSuccessRate + "%",
            currentSpeed: `${currentDelay}ms delay`,
            estimatedCompletionMinutes: newEstimatedTimeRemaining,
            optimization:
              consecutiveSuccessChunks >= 10 ? "FAST_MODE" : "NORMAL_MODE",
          });
        }

        // OPTIMIZED DELAY: Use current adaptive delay
        if (chunkNumber < totalChunks) {
          await this.delay(currentDelay);
        }
      } catch (error) {
        consecutiveSuccessChunks = 0;
        logger.error(`Chunk ${chunkNumber} failed with exception`, {
          error: error.message,
          chunkNumber,
          totalChunks,
          willSlowDown: true,
        });

        batchChunk.forEach((batch) => {
          results.failed += batch.length;
          batch.forEach((customerId) => {
            results.errors.push({
              customerId,
              error: "Chunk failed: " + error.message,
              chunk: chunkNumber,
              timestamp: new Date().toISOString(),
            });
          });
        });

        // After exception, slow down significantly
        currentDelay = Math.min(baseDelayBetweenChunks * 2, 10000);
        logger.warn(`Exception occurred - slowing down significantly`, {
          newDelay: currentDelay + "ms",
          safetyMode: "enabled",
        });

        if (chunkNumber < totalChunks) {
          await this.delay(currentDelay);
        }
      }
    }

    const finalSuccessRate =
      results.successful + results.failed > 0
        ? (
            (results.successful / (results.successful + results.failed)) *
            100
          ).toFixed(1)
        : "0";

    logger.info("OPTIMIZED rate-limited processing completed", {
      totalBatchesProcessed: totalProcessedBatches,
      totalSuccessful: results.successful,
      totalFailed: results.failed,
      finalSuccessRate: finalSuccessRate + "%",
      strategy: "Adaptive speed optimization",
      finalDelay: currentDelay + "ms",
      maxSpeedAchieved: consecutiveSuccessChunks >= 10 ? "YES" : "NO",
      performance:
        finalSuccessRate > 95
          ? "EXCELLENT"
          : finalSuccessRate > 85
          ? "GOOD"
          : "NEEDS_IMPROVEMENT",
    });
  }

  async processSingleTaggingBatch(
    batch,
    extractionTag,
    batchNumber,
    totalBatches
  ) {
    const startTime = Date.now();

    try {
      logger.debug(
        "Starting batch " +
          batchNumber +
          "/" +
          totalBatches +
          " (" +
          batch.length +
          " customers)"
      );

      const mutations = batch
        .map(
          (customerId, index) => `
        customer${index}: tagsAdd(
          id: "gid://shopify/Customer/${customerId}"
          tags: ["${extractionTag}"]
        ) {
          node {
            id
          }
          userErrors {
            field
            message
          }
        }
      `
        )
        .join("\n");

      const mutation = "mutation { " + mutations + " }";

      const response = await this.makeRequest("/graphql.json", "POST", {
        query: mutation,
      });

      const batchResult = { successful: 0, failed: 0, errors: [] };

      batch.forEach((customerId, index) => {
        const customerKey = "customer" + index;
        const result = response.data?.[customerKey];

        if (!result) {
          batchResult.failed++;
          batchResult.errors.push({
            customerId,
            error: "No result returned for " + customerKey,
            batchNumber,
          });
          return;
        }

        if (result.userErrors && result.userErrors.length > 0) {
          batchResult.failed++;
          batchResult.errors.push({
            customerId,
            error: result.userErrors[0].message,
            batchNumber,
          });
        } else {
          batchResult.successful++;
        }
      });

      const duration = Date.now() - startTime;
      logger.debug("Completed batch " + batchNumber + "/" + totalBatches, {
        batchSize: batch.length,
        successful: batchResult.successful,
        failed: batchResult.failed,
        durationMs: duration,
      });

      return batchResult;
    } catch (error) {
      logger.error("Batch " + batchNumber + " failed", {
        error: error.message,
        batchSize: batch.length,
        duration: Date.now() - startTime,
      });

      const batchResult = {
        successful: 0,
        failed: batch.length,
        errors: batch.map((customerId) => ({
          customerId,
          error: "Batch failed: " + error.message,
          batchNumber,
        })),
      };

      return batchResult;
    }
  }

  async retryFailedTagging(failedCustomerIds, tag, maxRetries = 2) {
    if (!failedCustomerIds || failedCustomerIds.length === 0) {
      return { success: true, remainingFailed: [], retryAttempts: 0 };
    }

    logger.info(
      `Starting retry process for ${failedCustomerIds.length} failed customer tags`,
      {
        maxRetries,
        tag,
        failedCount: failedCustomerIds.length,
      }
    );

    let currentFailedIds = [...failedCustomerIds]; // Create a copy
    let totalRetryAttempts = 0;
    const retryResults = [];

    for (let retry = 1; retry <= maxRetries; retry++) {
      if (currentFailedIds.length === 0) {
        logger.info("All customers successfully tagged during retry process");
        break;
      }

      totalRetryAttempts = retry;
      logger.info(
        `Retry attempt ${retry}/${maxRetries} for ${currentFailedIds.length} customers`,
        {
          attempt: retry,
          remainingCustomers: currentFailedIds.length,
          tag,
        }
      );

      try {
        // Use the existing bulkTagCustomers method for retry
        const retryResult = await this.bulkTagCustomers(
          currentFailedIds,
          tag,
          null
        );

        retryResults.push({
          attempt: retry,
          attempted: currentFailedIds.length,
          successful: retryResult.successful,
          failed: retryResult.failed,
          errors: retryResult.errors,
        });

        logger.info(`Retry attempt ${retry} completed`, {
          attempt: retry,
          attempted: currentFailedIds.length,
          successful: retryResult.successful,
          failed: retryResult.failed,
          successRate:
            ((retryResult.successful / currentFailedIds.length) * 100).toFixed(
              2
            ) + "%",
        });

        // Update the list of failed customers for next retry
        currentFailedIds = retryResult.errors.map((e) => e.customerId);

        if (currentFailedIds.length === 0) {
          logger.info(
            `All customers successfully tagged after ${retry} retry attempts`
          );
          return {
            success: true,
            remainingFailed: [],
            retryAttempts: totalRetryAttempts,
            retryResults,
            totalOriginalFailed: failedCustomerIds.length,
            totalRecovered: failedCustomerIds.length,
          };
        }
        // Exponential backoff before next retry
        if (retry < maxRetries) {
          const exponentialDelay = Math.min(
            5000 * Math.pow(2, retry - 1),
            maxFallBackTime
          );
          logger.info(
            "Waiting with exponential backoff before next retry attempt",
            {
              nextAttempt: retry + 1,
              exponentialDelay,
              delayMs: exponentialDelay + "ms",
            }
          );
          await this.delay(exponentialDelay);
        }
      } catch (error) {
        logger.error(`Retry attempt ${retry} failed with error`, {
          attempt: retry,
          error: error.message,
          remainingCustomers: currentFailedIds.length,
        });

        retryResults.push({
          attempt: retry,
          attempted: currentFailedIds.length,
          successful: 0,
          failed: currentFailedIds.length,
          error: error.message,
        });

        // If this is the last retry, break
        if (retry === maxRetries) {
          break;
        }

        // Wait before next retry even on error
        const delayMs = Math.min(
          5000 * Math.pow(2, retry - 1),
          maxFallBackTime
        );
        logger.info(
          `Waiting ${delayMs}ms before next retry attempt after error...`
        );
        await this.delay(delayMs);
      }
    }

    const finalResult = {
      success: currentFailedIds.length === 0,
      remainingFailed: currentFailedIds,
      retryAttempts: totalRetryAttempts,
      retryResults,
      totalOriginalFailed: failedCustomerIds.length,
      totalRecovered: failedCustomerIds.length - currentFailedIds.length,
      recoveryRate:
        (
          ((failedCustomerIds.length - currentFailedIds.length) /
            failedCustomerIds.length) *
          100
        ).toFixed(2) + "%",
    };

    if (currentFailedIds.length > 0) {
      logger.warn(
        `Retry process completed with ${currentFailedIds.length} customers still failing`,
        {
          originalFailed: failedCustomerIds.length,
          recovered: finalResult.totalRecovered,
          stillFailing: currentFailedIds.length,
          recoveryRate: finalResult.recoveryRate,
          retryAttempts: totalRetryAttempts,
        }
      );
    } else {
      logger.info(
        `Retry process successful - all ${failedCustomerIds.length} customers recovered`,
        {
          retryAttempts: totalRetryAttempts,
          recoveryRate: "100%",
        }
      );
    }

    return finalResult;
  }

  async extractCustomersByTag(tag, executionId) {
    const extractionTag = tag || config.EXTRACTION_TAG || "extracted-bulk";
    logger.info("Starting customer extraction with tag: " + extractionTag, {
      executionId,
    });

    const query = `
    {
      customers(query: "tag:${extractionTag}") {
        edges {
          node {${CUSTOMER_NODE_FIELDS}
          }
        }
      }
    }
  `;

    try {
      logger.info("Creating bulk operation for tag-based extraction", {
        tag: extractionTag,
        queryLength: query.length,
        executionId,
      });

      const bulkOperation = await this.createBulkOperation(query);

      if (!bulkOperation || !bulkOperation.id) {
        throw new Error("Failed to create bulk operation for tag extraction");
      }

      logger.info("Waiting for bulk operation completion", {
        operationId: bulkOperation.id,
        tag: extractionTag,
        executionId,
      });

      const completedOperation = await this.waitForBulkOperation(
        bulkOperation.id
      );

      if (!completedOperation) {
        throw new Error("Bulk operation failed to complete");
      }

      if (completedOperation.status === "FAILED") {
        throw new Error(
          "Bulk operation failed: " +
            (completedOperation.errorCode || "Unknown error")
        );
      }

      // CRITICAL FIX: Better handling of 0 customers found case
      if (completedOperation.objectCount === 0) {
        logger.warn("No customers found with the specified tag", {
          tag: extractionTag,
          operationId: completedOperation.id,
          status: completedOperation.status,
          possibleCauses: [
            "Tag propagation delay - tags not yet indexed",
            "All customers were untagged before extraction",
            "Tag name mismatch or typo",
          ],
          executionId,
        });

        // This is now treated as an error since we expect tagged customers
        throw new Error(
          `No customers found with tag '${extractionTag}' - this may indicate tag propagation delay or search index issues`
        );
      }

      if (!completedOperation.url) {
        logger.error("Bulk operation completed but no download URL provided", {
          operationId: completedOperation.id,
          status: completedOperation.status,
          objectCount: completedOperation.objectCount,
          tag: extractionTag,
          executionId,
        });

        throw new Error(
          "Bulk operation completed but no download URL provided for tag-based extraction"
        );
      }

      logger.info("Bulk operation successful, starting blob upload", {
        operationId: completedOperation.id,
        objectCount: completedOperation.objectCount,
        fileSize: completedOperation.fileSize,
        tag: extractionTag,
        executionId,
      });

      const blobResult = await this.downloadBulkDataWithFallback(
        completedOperation.url,
        extractionTag,
        "tag-based",
        executionId
      );

      if (!blobResult || !blobResult.success) {
        throw new Error("Failed to upload extracted data to blob storage");
      }

      logger.info(
        "Successfully streamed tagged customer data to Azure Blob Storage",
        {
          blobPath: blobResult.blobPath,
          blobUrl: blobResult.blobUrl,
          sizeMB: Math.round(blobResult.sizeBytes / (1024 * 1024)),
          customersFound: completedOperation.objectCount,
          tag: extractionTag,
          executionId,
        }
      );

      return { blobResult };
    } catch (error) {
      logger.error("Tag-based extraction failed", {
        error: error.message,
        stack: error.stack,
        tag: extractionTag,
        executionId,
        troubleshooting: {
          "0_customers_found":
            "Tag propagation delay - try increasing delay or use full extraction",
          no_download_url:
            "Shopify bulk operation issue - retry or use full extraction",
          bulk_operation_failed: "Query syntax or permission issue",
        },
      });

      // Re-throw with more context
      throw new Error(
        `Tag-based extraction failed for tag '${extractionTag}': ${error.message}`
      );
    }
  }

  validateDataConsistency(
    customerIdsCount,
    extractedCustomersCount,
    approach,
    additionalInfo = {}
  ) {
    logger.info("Validating data consistency", {
      approach,
      customerIdsCount,
      extractedCustomersCount,
      additionalInfo,
    });

    const validation = {
      approach,
      customerIdsCount,
      extractedCustomersCount,
      isValid: false,
      message: "",
      ratio: extractedCustomersCount / customerIdsCount,
      difference: extractedCustomersCount - customerIdsCount,
      additionalInfo,
    };

    if (approach === "tag-based") {
      validation.isValid = extractedCustomersCount <= customerIdsCount;

      if (validation.isValid) {
        if (extractedCustomersCount === customerIdsCount) {
          validation.message =
            "Perfect match: All customer IDs were successfully tagged and extracted";
        } else {
          validation.message =
            "Good result: " +
            extractedCustomersCount +
            "/" +
            customerIdsCount +
            " customers extracted (" +
            Math.round(validation.ratio * 100) +
            "%)";
        }
      } else {
        validation.message =
          "Warning: More customers extracted (" +
          extractedCustomersCount +
          ") than initial IDs (" +
          customerIdsCount +
          "). This may indicate new customers were added during processing.";
      }
    } else if (approach === "full") {
      validation.isValid = extractedCustomersCount >= customerIdsCount;

      if (validation.isValid) {
        if (extractedCustomersCount === customerIdsCount) {
          validation.message =
            "Perfect match: Customer count remained stable during extraction";
        } else {
          validation.message =
            "Expected result: More customers extracted (" +
            extractedCustomersCount +
            ") than initial count (" +
            customerIdsCount +
            "). " +
            validation.difference +
            " new customers likely added during processing.";
        }
      } else {
        validation.message =
          "Warning: Fewer customers extracted (" +
          extractedCustomersCount +
          ") than initial IDs (" +
          customerIdsCount +
          "). This is unexpected for full approach.";
      }
    } else if (approach === "date-based") {
      const { startDate, endDate } = additionalInfo;

      validation.isValid = true;

      if (extractedCustomersCount === customerIdsCount) {
        validation.message =
          "Perfect match: All customers (" +
          customerIdsCount +
          ") were updated within the specified date range";
      } else if (extractedCustomersCount < customerIdsCount) {
        validation.message =
          "Expected result: " +
          extractedCustomersCount +
          "/" +
          customerIdsCount +
          " customers were updated within date range (" +
          (startDate || "beginning") +
          " to " +
          (endDate || "now") +
          "). " +
          Math.abs(validation.difference) +
          " customers had no updates in this period.";
      } else {
        validation.message =
          "Interesting result: More customers extracted (" +
          extractedCustomersCount +
          ") than initial count (" +
          customerIdsCount +
          "). " +
          validation.difference +
          " additional customers were updated during processing.";
      }
    }

    logger.info("Data consistency validation completed", validation);
    return validation;
  }
}

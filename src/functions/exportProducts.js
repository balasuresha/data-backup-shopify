const { app } = require('@azure/functions');

app.http('exportProducts', {
    methods: ['GET', 'POST'],
    authLevel: 'function',
    route: 'exportProducts',
    handler: async (request, context) => {
        context.log('Export Products function processed a request.');
        
        try {
            // TODO: Move your existing logic from exportProducts/index.js here
            const result = await exportProductsLogic(request, context);
            
            return {
                status: 200,
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify(result)
            };
        } catch (error) {
            context.log.error('Error in exportProducts:', error);
            return {
                status: 500,
                body: JSON.stringify({ error: 'Internal server error' })
            };
        }
    }
});

async function exportProductsLogic(request, context) {
    // REPLACE THIS with your actual logic from exportProducts/index.js
    
    const method = request.method;
    const query = request.query;
    
    context.log(`Processing products export via ${method}`);
    
    // Your existing business logic goes here
    
    return {
        message: 'Products export completed',
        timestamp: new Date().toISOString(),
        method: method,
        processedRecords: 0 // Replace with actual count
    };
}
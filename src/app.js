const { app } = require('@azure/functions');
try {
    require('./functions/exportBulkCustomers');
    console.log('exportBulkCustomers loaded');
} catch (e) {
    console.error('exportBulkCustomers failed:', e.message);
}

try {
    require('./functions/exportBulkOrders');
    console.log('exportBulkOrders loaded');
} catch (e) {
    console.error('exportBulkOrders failed:', e.message);
}

try {
    require('./functions/exportProducts');
    console.log('exportProducts loaded');
} catch (e) {
    console.error('exportProducts failed:', e.message);
}

try {
    require('./functions/exportBulkOrdersTimer');
    console.log('exportBulkOrdersTimer loaded');
} catch (e) {
    console.error('exportBulkOrdersTimer failed:', e.message);
}
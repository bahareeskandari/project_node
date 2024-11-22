const fs = require('fs');
const path = require('path'); 
const { BigQuery } = require('@google-cloud/bigquery');
const ExcelJS = require('exceljs');


const bigquery = new BigQuery();

async function queryBigQuery() {
    // Define the updated query to filter by arrest and year 2004
    const queryTask_3_extended = `
        SELECT * 
        FROM \`alva-coding-test.chicago_crime.crime\`
        WHERE EXTRACT(YEAR FROM date) = 2004
          AND arrest = TRUE
        LIMIT 200
    `;

    try {
        const [rows] = await bigquery.query(queryTask_3_extended);

        // File path
        const filename = 'task_3_extended.xlsx';
        const filePath = path.join(__dirname, filename);

        // Delete the file if it already exists
        if (fs.existsSync(filePath)) {
            fs.unlinkSync(filePath);
        }

        // Create a new Excel file
        const workbook = new ExcelJS.Workbook();
        const worksheet = workbook.addWorksheet('task_3_extended'); 

        // Define the headers for the Excel file (use the keys from the first row)
        const columns = Object.keys(rows[0]).map(key => ({ header: key, key: key }));
        worksheet.columns = columns;

        // Add rows to the Excel sheet
        rows.forEach(row => {
            worksheet.addRow(row);
        });

        // Save the Excel file
        await workbook.xlsx.writeFile(filePath);

    } catch (error) {
        console.error('ERROR', error);
    }
}

queryBigQuery();

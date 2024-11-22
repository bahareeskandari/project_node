const fs = require('fs');
const path = require('path');
const { BigQuery } = require('@google-cloud/bigquery');
const ExcelJS = require('exceljs');  

const bigquery = new BigQuery();

async function queryBigQuery() {
    // Query to get the five most common crimes and their arrest rates
    const queryTask = `
        SELECT
            primary_type,
            COUNT(*) AS total_crimes,
            COUNTIF(arrest = TRUE) AS total_arrests,
            ROUND(SAFE_DIVIDE(COUNTIF(arrest = TRUE), COUNT(*)), 3) AS arrest_rate
        FROM
            \`alva-coding-test.chicago_crime.crime\`
        WHERE
            EXTRACT(YEAR FROM date) = 2020
        GROUP BY
            primary_type
        ORDER BY
            total_crimes DESC
        LIMIT 5;
    `;

    try {
        const [rows] = await bigquery.query(queryTask);

        // 5 items in the array with key name total_crimes that are added together.
        const totalCrimes = rows.reduce((sum, row) => sum + row.total_crimes, 0);

        const rowsToSelect = [];
        const maxRows = 200;

        for (const row of rows) {
            const crimeProportion = row.total_crimes / totalCrimes;
            const numRowsForCrime = Math.round(crimeProportion * maxRows);
            const queryCrimeData = `
                SELECT *
                FROM \`alva-coding-test.chicago_crime.crime\`
                WHERE EXTRACT(YEAR FROM date) = 2020
                  AND primary_type = '${row.primary_type}'
                LIMIT ${numRowsForCrime}
            `;
            
            const [crimeRows] = await bigquery.query(queryCrimeData);
            rowsToSelect.push(...crimeRows);
        }

        // File path
        const filename = 'task_4_extended.xlsx';
        const filePath = path.join(__dirname, filename);

        // Delete the file if it already exists
        if (fs.existsSync(filePath)) {
            fs.unlinkSync(filePath);  
        }

        // Create a new Excel file
        const workbook = new ExcelJS.Workbook();
        const worksheet = workbook.addWorksheet('Proportional Crime Data');

         // Define the headers for the Excel file (use the keys from the first row)
        const columns = Object.keys(rowsToSelect[0]).map(key => ({ header: key, key: key }));
        worksheet.columns = columns;

        // Add rows to the Excel sheet
        rowsToSelect.forEach(row => worksheet.addRow(row));

        // Save the Excel file
        await workbook.xlsx.writeFile(filePath);

    } catch (error) {
        console.error('ERROR', error);
    }
}

queryBigQuery();

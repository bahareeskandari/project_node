const fs = require('fs');
const path = require('path');
const { BigQuery } = require('@google-cloud/bigquery');
const ExcelJS = require('exceljs');  // Required for Excel writing

const bigquery = new BigQuery();

async function queryBigQuery() {
    // Query to get the five most common crimes in 2020
    const mainQuery = `
        SELECT
            primary_type,
            COUNT(*) AS total_crimes
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
        const [rows] = await bigquery.query(mainQuery);  // get top 5 crime types
        const totalCrimes = rows.reduce((sum, row) => sum + row.total_crimes, 0);  //  total number of crimes together

        const maxRows = 2000000;  //  total number of rows allowed in the Excel file

        // create the Excel file path  
        const filename = 'task_4_extended_large.xlsx';
        const filePath = path.join(__dirname, filename);

        //delete if it already exists
        if (fs.existsSync(filePath)){
            fs.unlinkSync(filePath)
        }

        // create a Excel workbook in streaming mode
        const workbook = new ExcelJS.stream.xlsx.WorkbookWriter({ filename: filePath });
        const worksheet = workbook.addWorksheet('Proportional Crime Data');

        // columns for the worksheet
        worksheet.columns = [
            { header: 'primary_type', key: 'primary_type' },
            { header: 'date', key: 'date' },
            { header: 'description', key: 'description' },
            { header: 'arrest', key: 'arrest' },
            { header: 'location', key: 'location' },
        ];

        // go through each crime type
        for (const row of rows) {
            const crimeProportion = row.total_crimes / totalCrimes; // percentage 
            const numRowsForCrime = Math.round(crimeProportion * maxRows);  // Calculate number of rows for this crime type

            const crimeQuery = `
                SELECT primary_type, date, description, arrest, location
                FROM \`alva-coding-test.chicago_crime.crime\`
                WHERE EXTRACT(YEAR FROM date) = 2020
                  AND primary_type = '${row.primary_type}'
            `;
            
            const options = { maxResults: 1000 };  // get in chunks of 1000 rows
            const stream = bigquery.createQueryStream(crimeQuery, options);

            // Stream results directly into the Excel file
            let rowsWritten = 0;
            for await (const crimeRow of stream) { 
                if (rowsWritten >= numRowsForCrime) break;  // Stop if the required rows for this crime type are written
                worksheet.addRow(crimeRow).commit();  // Write and commit each row to Excel
                // time consuming to commit for each row 
                rowsWritten++;
            }
        }

        // Finalize the workbook and save it to the file
        await workbook.commit();
        console.log('Excel file created:', filePath);

    } catch (error) {
        console.error('ERROR', error);
    }
}

queryBigQuery();

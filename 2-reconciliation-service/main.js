const csvParser = require('csv-parser');
const moment = require('moment');
const fs = require('fs');

async function main() {
  /** Input examples
   */
  const startDate = moment('2024-12-04').startOf('day').format('YYYY-MM-DD HH:mm:ss')
  const endDate = moment('2024-12-06').endOf('day').format('YYYY-MM-DD HH:mm:ss')
  const transactionData = await readCsv('transaction.csv', startDate, endDate)
  const bankStatementData = await readCsv('bank_statement.csv', startDate, endDate)

  /** Reconcile
   */
  const output = reconcileData(transactionData, bankStatementData)

  /** Output
   */
  console.log(output)
  return output
}

async function readCsv(path, startDate, endDate) {
  const data = []
  const inputStream = fs.createReadStream(`./2-reconciliation-service/csv/${path}`, 'utf8');
  await new Promise((resolve, reject) => {
    /** CSV read-parsing */
    inputStream
      .pipe(
        csvParser({
          mapHeaders: ({ header }) => header.trim(),
        })
      )
      .on('data', (row) => {

        /** Date filetr */
        const transactionDate = moment(row.transactionTime).format('YYYY-MM-DD HH:mm:ss');
        const isInDateRange = moment(transactionDate).isBetween(startDate, endDate);

        if (isInDateRange) {
          /** file filter (system/bank transaction) */
          if (row.trxID) {
            data.push({
              trxID: row.trxID,
              amount: parseFloat(row.amount),
              type: row.type,
              transactionTime: moment(row.transactionTime).format('YYYY-MM-DD HH:mm:ss'),
            });
          } else {
            data.push({
              id: row.id,
              amount: parseFloat(row.amount),
              transactionTime: moment(row.transactionTime).format('YYYY-MM-DD HH:mm:ss'),
            });
          }
        }
      })
      .on('end', resolve)
      .on('error', reject);
  });
  return data
}

function reconcileData (systemTrx, bankTrx) {
  /** forming debit-credit amount value */
  systemTrx.forEach((data, i)=> {
    if (data.type === 'DEBIT') {
      systemTrx[i].amount = 0 - data.amount
    }
  })

  /** find matched trx */
  const matchedTrx = [];
  systemTrx.forEach((systemData) => {
    const bankData = bankTrx.find((x) => x.amount === systemData.amount && x.transactionTime === systemData.transactionTime);
    if (bankData) {
      matchedTrx.push({ systemData, bankData });
    }
  });

  /** find UNmatched trx */
  const unmatchedTransactions = systemTrx.filter((x) => {
    return !bankTrx.some((y) => y.amount === x.amount && y.transactionTime === x.transactionTime)
  });

  /** count discrepancies */
  let discrepancies = 0;
  matchedTrx.forEach(({ systemData, bankData }) => {
    discrepancies += Math.abs(systemData.amount - bankData.amount);
  });

  return {
    totalProcessed: {
      total: systemTrx.length + bankTrx.length,
      details: {
        transaction: systemTrx.length,
        bank: bankTrx.length,
      }
    },
    matchedTransaction: matchedTrx.length,
    unmatchedTransaction: unmatchedTransactions.length,
    discrepancies,
  }
}

main()
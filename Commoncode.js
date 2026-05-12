const logger = require('./loggerConfig.js');
const { MongoClient, ServerApiVersion } = require("mongodb");
const axios = require('axios');
const AWS = require("aws-sdk");
const fs = require("fs");
const path = require('path');
const { scraperMap } = require('./ScraperConfig.js');

const config = JSON.parse(
    fs.readFileSync(path.resolve('./config/config.json'), 'utf8')
);

const { USERNAME, PASSWORD, Host } = config.nexusConfig;

const awsConfig = config.awsConfig;
const mongoConfig = config.mongoConfig;
const mongoLogConfig = config.mongoConfig;
const mode = config.mode;
const addParamKey = config.SOS.ADDITIONAL_PARAM_KEY;

const logClient = new MongoClient(mongoLogConfig?.MONGO_URI_LOGS, {
    useNewUrlParser: true,
    serverApi: {
        version: ServerApiVersion.v1,
        strict: true,
        deprecationErrors: true,
    },
});

let mockMessages = [
    {
        MessageId: '3bfa9074-1fcf-4343-ba1e-a057ada48262',
        ReceiptHandle: 'AQEBL3AHFQR92uZzqvdQXKG57/cd2igYA8AmL8sAyVY4xXtwyFm+KxXHRy+W+H4S/tTqtnIVByMYoGeyKzurbZe0LT/6ioYn5nm8g/xPP98vl2zmPa/7HzXPtv1QC7FkLwc4VGuW0kbwS32V3v2DdSKQqdk157PRGdEmp3MRBpof8TM9q8qxp0CoCLvEhrk9lzVoUJfrqCw1Cao8ZPgAQTCY8RoXs1hfSSY4GUsctoZpqOa6DP67F34ZKqn7f6Y0boksdqaEsBw8mq5pVnRMids8sO6+9Y4fhbV++XP9W9Xjckgg=',
        MD5OfBody: '684397856bb923ba2500bd1337a838d4',
        Body: '[{"accountId":"66ab0af3ce368071add691fd", "url": "https://builtwith.com/avivomed.com", "source": "builtwith.com", "id": "67d631db9feb150b266d07a6", "checkId":"67d631db9feb150b266d07a6", "checkType": "builtwith", "scraperType": "company", "typeFreq": "SCRAPPER#1440"}]',
        //Body: '[{"stateAbbreviation":"CA","businessId":"699474","internalId":null,"companyId":"66bb65c3e400af3bd6e6add7","id":"66bb65c3e400af3bd6e6add7","domain":"dell.com","checkType":"CA","scraperType":"COMPANY_SOS","typeFreq":"SOS#1440"}]'
        //Body: '[{"accId":"660a1ad24364ce635a3d6b68","url":"https://builtwith.com/avivomed.com","source":"builtwith.com","id":"66857330f61e18364e5a0cf0","checkId":"66857330f61e18364e5a0cf0", "checkType":"builtwith","checkName":"builtwith","scraperType":"review", "typeFreq":"SCRAPERS#1440"}]'
    }
];

const client = new MongoClient(mongoConfig?.MONGO_URI, {
    useNewUrlParser: true,
    serverApi: {
        version: ServerApiVersion.v1,
        strict: true,
        deprecationErrors: true,
    },
});

AWS.config.update({
    accessKeyId: awsConfig.ACCESS_KEY_ID,
    secretAccessKey: awsConfig.SECRET_ACCESS_KEY,
    region: awsConfig.AWS_REGION,
});

const sqs = new AWS.SQS();
//const queueUrl = awsConfig.SQS_QUEUE_URL;

var sessionId, isRunning = false;
let isRunningScraper = false;
let isRunningSOS = false;
let isRunningCompany = false;
let refreshing = null;

/*
Start point
*/

// 1. Initialize MongoDB only once
async function initMongoConnections() {
    try {
        if (!client.topology?.isConnected()) {
            await client.connect();
            logger.info('✅ Main MongoDB connected');
        }
        if (!logClient.topology?.isConnected()) {
            await logClient.connect();
            logger.info('✅ Log MongoDB connected');
        }
    } catch (err) {
        logger.info('❌ MongoDB init error:', err);
    }
}

// 2. Start processing
const companyQueue = "https://sqs.us-east-1.amazonaws.com/437248432885/CompanyJobQueue.fifo";
const scraperQueue = "https://sqs.us-east-1.amazonaws.com/437248432885/ScraperQueue.fifo";
const sosQueue = "https://sqs.us-east-1.amazonaws.com/437248432885/SOSJobQueue.fifo";

(async () => {
    //await initMongoConnections();

    setInterval(() => receiveAndProcessMessages(companyQueue, 'isRunningCompany', 2), 5000);
    //setInterval(() => receiveAndProcessMessages(scraperQueue, 'isRunningScraper', 2), 5000);
   // setInterval(() => receiveAndProcessMessages(sosQueue, 'isRunningSOS', 1), 5000);
})();


async function getSessionId() {
    const resp = await axios.post(`${Host}/nexus/v1/login`, {
        email: USERNAME,
        password: PASSWORD,
    }, { validateStatus: () => true });

    if (resp.status !== 200 || !resp?.data?.data?.sessionId) {
        throw new Error(`login failed (status ${resp.status})`);
    }
    return resp.data.data.sessionId;
}

/** Ensures we have a session; coalesces concurrent refreshes into one. */
async function ensureSessionId(force = false) {
    if (!force && sessionId) return sessionId;

    // if a refresh is already underway, await it
    if (refreshing) {
        await refreshing;
        return sessionId;
    }

    refreshing = (async () => {
        try {
            sessionId = await getSessionId();
            return sessionId;
        } finally {
            refreshing = null;
        }
    })();

    await refreshing;
    return sessionId;
}

/** Core POST with current session (no retry). */
// async function postReviewsOnce(data) {
//     try {
//         console.log('insertig reviews', data?.length);
//         const sid = await ensureSessionId();
//         return axios.post(
//             `${Host}/nexus/v1/reviews`,
//             data,
//             {
//                 headers: { sessionId: sid },
//                 validateStatus: () => true, // we handle statuses manually
//                 timeout: 30_000,
//             }
//         );
//     } catch (error) {
//         console.error('Error', error);
//         return [];
//     }

// }

async function receiveAndProcessMessages(queueUrl, flagName, num_message) {
    if (global[flagName]) return; // another cycle already running
    global[flagName] = true;

    try {
        const params = {
            QueueUrl: queueUrl,
            MaxNumberOfMessages: num_message,   // ✅ Fetch 2 messages at a time
            VisibilityTimeout: 300,
            WaitTimeSeconds: 10
        };

        console.log(`Fetching messages for ${flagName}`);
        const response = (mode === 'prod')
            ? await sqs.receiveMessage(params).promise()
            : { Messages: mockMessages.slice(0, 2) }; // ✅ simulate 2 mock messages in dev mode

        const Messages = response.Messages || [];
        if (!Messages.length) {
            console.log(`No Message in ${flagName}`);
            global[flagName] = false;
            return;
        }

        console.log(`📥 [${queueUrl}] Fetched ${Messages.length} messages`);

        // ✅ Process all messages concurrently
        await Promise.all(
            Messages.map(async (message) => {
                try {
                    const bodyArray = JSON.parse(message.Body);
                    if (Array.isArray(bodyArray) && bodyArray.length > 0) {
                        for (const iterator of bodyArray) {
                            if (!sessionId) sessionId = await getSessionId();
                            const htmlContent = iterator.scraperType === 'COMPANY_SOS' ? '' : await fetchHTMLContent(iterator);
                            //✅ Save HTML content to file (for debug/logging)
                            if (htmlContent) {
                                saveHTMLToFile(htmlContent);
                            }
                            await ProcessHTML(iterator, htmlContent);
                        }
                    }

                    // ✅ Delete each message after processing
                    if (mode === 'prod') await deleteProcessedMessageFromSQS(queueUrl, message);
                } catch (err) {
                    console.error(`[${queueUrl}] Message Processing Error:`, err);
                }
            })
        );

    } catch (error) {
        console.error(`[${queueUrl}] Error:`, error);
    } finally {
        global[flagName] = false; // allow next poll
    }
}

async function deleteProcessedMessageFromSQS(queueUrl, message) {
    console.log('Deleting message ReceiptHandle = ', message.ReceiptHandle);
    logger.info('Deleting message ReceiptHandle = ', message.ReceiptHandle);
    const deleteParams = {
        QueueUrl: queueUrl,
        ReceiptHandle: message.ReceiptHandle,
    };

    try {
        await sqs.deleteMessage(deleteParams).promise();
        console.log("Deleted processed message from SQS.");
        logger.info("Deleted processed message from SQS.");
    } catch (error) {
        console.error("Error deleting message from SQS:", error);
        logger.info("Error deleting message from SQS:", error);
    }
}

// ✅ Graceful shutdown
process.on('SIGINT', async () => {
    console.log('🔴 Shutting down...');
    //intervals.forEach(clearInterval);
    await client.close();
    await logClient.close();
    process.exit(0);
});

process.on('SIGTERM', async () => {
    console.log('🔴 Shutting down...');
    //intervals.forEach(clearInterval);
    await client.close();
    await logClient.close();
    process.exit(0);
});

const saveHTMLToFile = (htmlContent) => {
    try {
        const filePath = path.join(__dirname, 'html.json'); // Save in the current directory
        fs.writeFileSync(filePath, JSON.stringify({ content: htmlContent }, null, 2), 'utf8');
        console.log('✅ HTML content saved successfully in html.json');
    } catch (error) {
        console.error('❌ Error saving HTML to file:', error);
    }
};

async function fetchHTMLContent(param, retry = true) {
    try {
        console.log("Fetching HTML for:", param.url, param.scraperType);
        logger.info("Fetching HTML for:", param.url, param.scraperType);

        const resp = await axios.post(
            `${Host}/nexus/v1/webpages`,
            {
                url: param.url,
                type: param.scraperType,
            },
            {
                headers: { sessionId },
            }
        );

        const html = resp?.data?.data?.html || "";
        return { status: true, code: 200, html };
    } catch (error) {
        const isAxios = axios.isAxiosError(error);
        const status = isAxios ? error.response?.status : null;

        // ✅ If session expired / invalid, refresh and retry once
        if (status === 401 && retry) {
            try {
                logger.warn("401 received. Refreshing sessionId and retrying...", {
                    url: param.url,
                    type: param.scraperType,
                });

                await ensureSessionId(true); // refresh sessionId
                return await fetchHTMLContent(param, false); // retry once with same param
            } catch (e) {
                logger.error("Session refresh failed:", e?.message || e);
                // fallthrough to normal error formatting below
            }
        }

        const err = isAxios
            ? {
                message: error.message,
                code: error.code,
                status: error.response?.status,
                url: error.config?.url,
                method: error.config?.method,
                html: "",
            }
            : { message: String(error) };

        console.error("Fetch html error:", JSON.stringify(err));
        return err;
    }
}

async function receiveAndProcessMessages(queueUrl, flagName, num_message) {
    if (global[flagName]) return;
    global[flagName] = true;

    try {

        // -------- receiveMessage ----------
        let response = {};
        try {
            const params = {
                QueueUrl: queueUrl,
                MaxNumberOfMessages: num_message,
                VisibilityTimeout: 300,
                WaitTimeSeconds: 10
            };

            console.log(`Fetching messages for ${flagName}`);
            response = (mode === 'prod')
                ? await sqs.receiveMessage(params).promise()
                : { Messages: mockMessages.slice(0, 2) };

        } catch (err) {
            console.error(`[${queueUrl}] receiveMessage Error:`, err);
            return;
        }

        const Messages = response.Messages || [];
        if (!Messages.length) {
            console.log(`No Message in ${flagName}`);
            return;
        }

        console.log(`📥 [${queueUrl}] Fetched ${Messages.length} messages`);

        // -------- Process messages ----------
        await Promise.all(
            Messages.map(async (message) => {
                try {
                    let bodyArray = [];
                    try {
                        bodyArray = JSON.parse(message.Body);
                    } catch (err) {
                        console.error('JSON parse error:', err);
                        return;
                    }

                    if (Array.isArray(bodyArray) && bodyArray.length > 0) {

                        for (const iterator of bodyArray) {

                            // ------- getSessionId ----------
                            try {
                                if (!sessionId) sessionId = await getSessionId();
                            } catch (err) {
                                console.error('getSessionId Error:', err);
                            }

                            let htmlres = {};

                            // ------- fetchHTMLContent ----------
                            let htmlContent = '';
                             try {
                                htmlres = iterator.scraperType === 'COMPANY_SOS'
                                    ? {}
                                    : await fetchHTMLContent(iterator);
                                if (Object.keys(htmlres).length > 0
                                ) {
                                    htmlContent = htmlres?.html;
                                }
                            } catch (err) {
                                console.error('fetchHTMLContent Error:', err);
                            } 

                            // ------- saveHTMLToFile ----------
                            if (htmlContent) {
                                try {
                                    saveHTMLToFile(htmlContent);
                                } catch (err) {
                                    console.error('saveHTMLToFile Error:', err);
                                }
                            }

                            // ------- ProcessHTML ----------
                            try {
                                await ProcessHTML(iterator, htmlContent);
                            } catch (err) {
                                console.error('ProcessHTML Error:', err);
                            }
                        }
                    }

                    // ------- deleteProcessedMessageFromSQS ----------
                    try {
                        if (mode === 'prod') await deleteProcessedMessageFromSQS(queueUrl, message);
                    } catch (err) {
                        console.error('deleteProcessedMessageFromSQS Error:', err);
                    }

                } catch (msgErr) {
                    console.error(`[${queueUrl}] Message Processing Wrapper Error:`, msgErr);
                }
            })
        );

    } catch (error) {
        console.error(`[${queueUrl}] Outer Error:`, error);
    } finally {
        global[flagName] = false;
    }
}


/* function readJSON(filePath) {
    try {
        if (!fs.existsSync(filePath)) {
            throw new Error(`File not found: ${filePath}`);
        }
        const fileContent = fs.readFileSync(filePath, "utf-8");
        return JSON.parse(fileContent);
    } catch (error) {
        console.error(`❌ Error reading ${filePath}:`, error.message);
        return null;
    }
}

function saveJSON(filePath, data) {
    try {
        fs.writeFileSync(filePath, JSON.stringify(data, null, 2), "utf-8");
        console.log(`✅ Data saved to ${filePath}`);
    } catch (error) {
        console.error(`❌ Error saving ${filePath}:`, error.message);
    }
} */

const ProcessHTML = async (payload, htmlContent = "") => {
    try {
        let htmlRes;
        //const { checkType, url, id, source, scraperType } = payload;
        const { checkType, url = '', id, source, scraperType, stateAbbreviation = '', businessId = '', internalId = '' } = payload;
        const type = checkType?.toLowerCase();

        if (type === undefined) {
            return;
        }
        console.log('type-=-', type);
        logger.info('type-=-', type);
        console.log('payload=-=-', payload);
        logger.info('payload=-=-', payload);
        const scraperConfig = scraperMap[type];
        // if (!scraperConfig || !scraperConfig.match) {
        //     console.warn(`Unsupported or unmatched scraper: ${payload.scarpername}`);
        //     return;
        // }


        // Make extractor async to support both HTML-based and SOS-type scraping
        let scraperRes, logMessage = '';
        let res;
        let reviewRes;
        let stagehandMessage = '';
        let htmlResMessage = '';
        let isHTMLFetch = true;
        if (scraperType === 'COMPANY_SOS') {
            scraperRes = []
            let payloadObj = { state: stateAbbreviation, businessID: businessId };
            if (stateAbbreviation?.toUpperCase() === 'TX') {
                console.log('businessId-=-=', businessId, businessId?.length);
                logger.info('businessId-=-=', businessId, businessId?.length);
                if (businessId?.length === 9 || businessId?.length === 11) {
                    payloadObj['additionalParams'] = { "searchID": "taxpayerNumber" };
                } else {
                    payloadObj['additionalParams'] = { "searchID": "fileNumber" };
                }
            } else
                if (internalId && Object.keys(addParamKey).indexOf(stateAbbreviation?.toUpperCase()) > -1) {
                    payloadObj['additionalParams'] = {};
                    payloadObj['additionalParams'][addParamKey[stateAbbreviation?.toUpperCase()]] = internalId;
                }

            // ✅ Try Stagehand API first
            let APIRes = await fetchSOSDataFromAPI(id, payloadObj, stateAbbreviation);
            console.log('Stagehand API Response:', APIRes);
            logger.info('Stagehand API Response:', APIRes);

            if (APIRes?.status) {
                scraperRes = APIRes?.data;
                logMessage = 'Fetched data using Stagehand API';
            } else {
                // ✅ Stagehand failed, fallback to extractor
                console.log('Stagehand failed, trying extractor...');
                logger.info('Stagehand failed, trying extractor...');
                if (scraperConfig && scraperConfig.extractor) {
                    scraperRes = await scraperConfig.extractor(payload);
                    if (scraperRes?.length > 0) {
                        logMessage = 'Fetched data using local extractor';
                    } else {
                        scraperRes = [];
                        logMessage = APIRes?.message || 'Extractor also failed';
                    }
                } else {

                }
            }

        }
        else if (scraperType === 'review') {
            res = await fetchFromAPI(url, payload, 10);

            console.log('review API status', res.status, res?.scraperMessage);
            logger.info('review API status', res.status, res?.scraperMessage);
            stagehandMessage = res?.scraperMessage;
            if (!res.status || res?.data?.length === 0) {

                htmlContent = '';
                try {
                    console.log('Checking node scraper exist');
                    if (scraperConfig !== undefined && scraperConfig.extractor !== undefined) {
                        console.log('Fetch HTML');
                        logger.info('Fetch HTML');
                        htmlRes = await fetchHTMLContent(payload);
                        htmlContent = htmlRes?.html || "";
                        console.log('payload', payload);
                        let revRes = await scraperConfig.extractor(htmlContent, payload);
                        scraperRes = revRes?.reviews || []
                        if (revRes?.reviews?.length === 0) {
                            htmlResMessage = revRes?.message;
                        }
                    } else {
                        htmlResMessage = 'Node scraper not found';
                    }

                    console.log('scraperRes-=-=-', scraperRes);
                } catch (error) {
                    console.log(error)
                    logger.info(error)
                    scraperRes = [];
                }
                if (htmlContent == "") {
                    isHTMLFetch = false
                }
                if (scraperConfig !== undefined && scraperConfig.extractor !== undefined) {
                    res = await processReviews(scraperRes, payload);
                }

            }
            // console.log('res?.data-==-',res?.data);
            reviewRes = await insertReviews(res?.data);

            console.log('Review insert response:', reviewRes);
            logger.info('Review insert response:', reviewRes);
            console.log('res?.incidentObj-=-=', res?.incidentObj);


            if (
                res?.incidentObj &&
                Object.keys(res.incidentObj).length > 0
            ) {
                console.log('sessionId0', sessionId);
                const reviewIncident = await insertIncident(sessionId, res.incidentObj);
                console.log('Incident insert response:', reviewIncident);
                logger.info('Incident insert response:', reviewIncident);
            }
        } else {
            //  scraperRes = await scraperConfig.extractor(htmlContent, payload);
            //  res = await fetchFromAPI(url, payload, 10);

            let companyRes;
            if (url.indexOf('owler.com') > -1 || url.indexOf('pitchbook.com') > -1) {
                try {
                    companyRes = await axios.post(`http://54.224.20.169:8890/api/company`, { url });
                    console.log('companyRes-=-=', companyRes.data);
                    logger.info('companyRes-=-=', companyRes.data);

                    if (Object.keys(companyRes?.data?.data).length > 0) {
                        scraperRes = [{ ...companyRes?.data?.data, companyId: id?.replace('companyId:', '') }];
                        stagehandMessage = 'Stagehand successful';
                    } else {
                        stagehandMessage = 'Stagehand failed';
                    }
                } catch (error) {
                    console.log('Error stagehand', error);
                    logger.info('Error stagehand', error);
                    stagehandMessage = 'Stagehand failed';
                }
            }

            if (!companyRes?.data?.success || (Object.keys(companyRes?.data?.data).length == 0)) {
                console.log("data in...");

                try {
                    if (htmlContent === "") {
                        htmlRes = await fetchHTMLContent(payload);
                        htmlContent = htmlRes?.html;
                    }
                    //console.log("html content..", htmlContent);
                    scraperRes = await scraperConfig.extractor(htmlContent, payload);
                    console.log('scraperRes-=-=-', scraperRes);
                    logger.info('scraperRes-=-=-', scraperRes);

                } catch (error) {
                    console.error("error... ", error);
                    scraperRes = [];
                }
                if (htmlContent == "") {
                    isHTMLFetch = false
                }
            }

        }

        //console.log('scraper res=-==', scraperRes, Array.isArray(scraperRes) ? scraperRes.length : scraperRes);
        if (Array.isArray(scraperRes)) {
            logger.info('scraper res=-==', scraperRes[0], scraperRes.length);
        }


        if (scraperType === 'COMPANY_SOS') {
            res = { data: scraperRes || [] };
            //if(scraperRes?.length === 0){
            console.log('logMessage=--==', logMessage);
            logger.info('logMessage=--==', logMessage);
            res['scraperMessage'] = logMessage;
            // }
            reviewRes = await insertSOSData(stateAbbreviation, scraperRes);
            console.log('SOS-0--', stateAbbreviation, res?.data?.length, reviewRes?.data?.length);
            logger.info('SOS-0--', stateAbbreviation, res?.data?.length, reviewRes?.data?.length);
            if (res?.data?.length > 0) {
                await updateScraperDate(stateAbbreviation);
            }
        } else if (scraperType !== 'review' && scraperType !== 'COMPANY_SOS') {
            res = { data: scraperRes || [] };
            //console.log('company data=-=-', res);


            if (scraperRes?.length > 0) {
                res['scraperMessage'] = 'Node Successful';
                if (source == "finance.yahoo.com") {
                    reviewRes = await insertCompanyData(source, [scraperRes[0]]);
                    if (scraperRes?.length > 1) {
                        await insertCompanyData("PVTValuation.com", [scraperRes[1]]);
                    }
                }
                else {
                    reviewRes = await insertCompanyData(source, scraperRes);
                    console.log('reviewRes-0--', res?.data?.length, reviewRes?.data?.length);
                    logger.info('reviewRes-0--', res?.data?.length, reviewRes?.data?.length);
                }
            } else {
                res['scraperMessage'] = 'Node Failed';
            }
        }


        // const scraperRes = (scraperType === 'SOS')?[]: scraperConfig.extractor(htmlContent, payload);
        // let res;

        // // Insert Reviews
        // console.log('scraper res=-==', scraperRes.length);
        // // if (mode === 'dev'){
        // //     return;
        // // }
        // let reviewRes;
        // if (scraperType === 'review') {
        //     if (Array.isArray(scraperRes)) {
        //         res = await processReviews(scraperRes, payload);
        //     } else {
        //         res = scraperRes;
        //     }
        //     console.log("data..", res);
        //     //return
        //     reviewRes = await insertReviews(sessionId, res?.data);
        //     console.log('Review insert response:', reviewRes);

        //     // Insert Incidents (if any)
        //     if (
        //         res?.incidentObj &&
        //         res.incidentObj.reason && // Only send if reason is non-empty
        //         Object.keys(res.incidentObj).length > 0
        //     ) {
        //         const reviewIncident = await insertIncident(sessionId, res.incidentObj);
        //         console.log('Incident insert response:', reviewIncident);
        //     }

        // } else if(scraperType === 'SOS'){

        //         let resp = await scrapeTexasCompanyData(payload); 
        //         console.log('res=-=-',resp);
        //         res = {data: resp || []};
        //         reviewRes = await insertSOSData(stateAbbreviation, resp);
        //         console.log('reviewRes-0--', res?.length, reviewRes?.data?.length);


        // }else {
        //     res = { data: scraperRes || [] };
        //     console.log('company data=-=-', res);
        //     if (scraperRes?.length > 0) {
        //         reviewRes = await insertCompanyData(source, scraperRes);
        //         console.log('reviewRes-0--', res?.data?.length, reviewRes?.data?.length);
        //     }
        // }

        // Insert Scraper Logs
        console.log('res?.scraperMessage--=-', res?.scraperMessage, stagehandMessage, res?.data?.length);
        logger.info('res?.scraperMessage--=-', res?.scraperMessage, stagehandMessage, res?.data?.length);

        if (res?.scraperMessage?.indexOf('Stagehand successful') > -1 && stagehandMessage?.indexOf('Stagehand successful') > -1) {
            stagehandMessage = '';
            isHTMLFetch = null;
        } else if (res?.scraperMessage?.indexOf('Stagehand failed') > -1 && stagehandMessage?.indexOf('Stagehand failed') > -1) {
            stagehandMessage = '';
        }
        let FinalMessage = '';
        if (isHTMLFetch) {
            FinalMessage += 'HTML Fetched;';
        } else {
            if (htmlResMessage !== 'Node scraper not found' && isHTMLFetch !== null) {
                FinalMessage += `${JSON.stringify(htmlRes)};HTML Not Fetched;\n`;
            }

        }

        FinalMessage += res?.data?.length === 0 ? `${stagehandMessage} ${res?.scraperMessage || ''}` : `${stagehandMessage} ${res?.scraperMessage || ''}`;

        const scraperResult = [{
            'url': url || `${stateAbbreviation}${businessId || ''}${internalId || ''}`,
            count: res?.data?.length || 0,
            insertCount: reviewRes?.data?.length || 0,
            id,
            message: `${FinalMessage};${htmlResMessage}`,
            scraper_type: `${scraperType}_${source || type}`,
            status: true,
            ts: new Date().toISOString(),
            type: "RESPONSE"
        }];


        await insertAppLogs(scraperResult);

        if (res?.data?.length > 0) {
            await updateExtractionDate(source);
        }

    } catch (error) {
        console.error('Error in ProcessHTML:', error);
        logger.info('Error in ProcessHTML:', error);
    }
};



// --- helpers ---
function subDays(d, n) { const x = new Date(d); x.setDate(x.getDate() - n); return x; }
function subMonths(d, n) { const x = new Date(d); x.setMonth(x.getMonth() - n); return x; }
function subYears(d, n) { const x = new Date(d); x.setFullYear(x.getFullYear() - n); return x; }

const MONTHS = {
    jan: 0, january: 0,
    feb: 1, february: 1,
    mar: 2, march: 2,
    apr: 3, april: 3,
    may: 4,
    jun: 5, june: 5,
    jul: 6, july: 6,
    aug: 7, august: 7,
    sep: 8, sept: 8, september: 8,
    oct: 9, october: 9,
    nov: 10, november: 10,
    dec: 11, december: 11,
};

function normalize(str) {
    return String(str)
        .replace(/\u00A0/g, " ") // NBSP -> space
        .replace(/\s+/g, " ")    // collapse
        .trim();
}

function safeParseReviewDate(raw) {
    if (!raw || typeof raw !== "string") return null;
    const norm = normalize(raw);
    const s = norm.toLowerCase();

    // Common literals
    if (s === "today") return new Date();
    if (s === "yesterday") return subDays(new Date(), 1);

    // Relative: "4 days ago", "2 years ago"
    {
        const m = s.match(/(?:less\s+than\s+)?(\d+)\s+(day|days|month|months|year|years)\s+ago/);
        if (m) {
            const n = parseInt(m[1], 10);
            const unit = m[2];
            const now = new Date();
            if (unit.startsWith("day")) return subDays(now, n);
            if (unit.startsWith("month")) return subMonths(now, n);
            if (unit.startsWith("year")) return subYears(now, n);
        }
    }

    // "Dined X days ago" / "Dined on August 4, 2025"
    {
        const m = s.match(/\bdined\s+(?:about\s+)?(\d+)\s+(day|days|month|months|year|years)\s+ago\b/);
        if (m) {
            const n = parseInt(m[1], 10);
            const now = new Date();
            if (m[2].startsWith("day")) return subDays(now, n);
            if (m[2].startsWith("month")) return subMonths(now, n);
            if (m[2].startsWith("year")) return subYears(now, n);
        }
        const m2 = s.match(/\bdined\s+on\s+(.+)$/);
        if (m2) {
            const parsed = parseMonthDayYear(m2[1]);
            if (parsed) return parsed;
            const d = new Date(m2[1]);
            if (!isNaN(d)) return d;
        }
    }

    // "Reviewed on 14 June 2025"
    {
        const m = s.match(/\breviewed(?:\s*on)?\s*:?\s+(.+)$/);
        if (m) {
            const parsed = parseMonthDayYear(m[1]);
            if (parsed) return parsed;
            const d = new Date(m[1]);
            if (!isNaN(d)) return d;
        }
    }

    // NEW: "Thursday, August 14, 2025 at 09:48:22 AM"
    {
        const m = s.match(/^[a-z]+,\s+([a-z]+\s+\d{1,2},\s+\d{4})(?:\s+at\s+([\d:]+\s*(am|pm)?))?$/i);
        if (m) {
            const datePart = m[1];
            const timePart = m[2] || "";
            const d = new Date(`${datePart} ${timePart}`);
            if (!isNaN(d)) return d;
        }
    }

    // Explicit "Aug 10, 2025" / "Jul 23 2025"
    {
        const parsed = parseMonthDayYear(norm);
        if (parsed) return parsed;
    }

    // ISO-like "2025-08-10"
    {
        const m = s.match(/^(\d{4})-(\d{2})-(\d{2})$/);
        if (m) {
            const y = +m[1], mo = +m[2] - 1, d = +m[3];
            const dt = new Date(y, mo, d);
            if (!isNaN(dt)) return dt;
        }
    }

    // Last resort
    const dt = new Date(norm);
    if (!isNaN(dt)) return dt;

    return null;
}

function parseMonthDayYear(input) {
    const txt = normalize(input).toLowerCase();

    // 1) Month DD, YYYY or Month DD YYYY
    let m = txt.match(/^([a-z]+)\s+(\d{1,2})(?:,)?\s+(\d{4})$/);
    if (m) {
        const mon = MONTHS[m[1]];
        const dd = parseInt(m[2], 10);
        const yy = parseInt(m[3], 10);
        if (mon != null) return new Date(yy, mon, dd);
    }

    // 2) DD Month YYYY
    m = txt.match(/^(\d{1,2})\s+([a-z]+)\s+(\d{4})$/);
    if (m) {
        const dd = parseInt(m[1], 10);
        const mon = MONTHS[m[2]];
        const yy = parseInt(m[3], 10);
        if (mon != null) return new Date(yy, mon, dd);
    }

    return null;
}

// ---- your function ----
async function fetchFromAPI(url, payload, num_reviews = 10) {
    try {
        console.log('Try to fetch from API');
        logger.info('Try to fetch from API');
        const resp = await axios.post(
            'http://54.224.20.169:8890/api/review',
            { url, num_reviews }
        );

        const api = resp?.data;
        //console.log('review data-=-=-', api?.reviews || []);
        const reviewdata = (api?.reviews || []).map(item => {
            const parsed = safeParseReviewDate(item.date);
            return {
                title: item.title,
                ratings: item.rating,
                author: item.author,
                // keep ISO if parsed, else null; also keep the original for reference
                date: parsed ? parsed.toISOString() : null,
                // rawDate: item.date,
                description: item.description,
                sourceCollector: payload.source,
                sourceUrl: payload.url,
                checkId: payload.checkId,
                companyId: (payload.checkId?.indexOf('companyId') > -1) ? payload.checkId?.replace('companyId:', '') : null
            };
        });

        console.log('reviewdata-=-', reviewdata.length);
        logger.info('reviewdata-=-', reviewdata.length);
        return await processReviews(reviewdata, payload, 'stagehand');

    } catch (error) {
        console.error('Unable to fetch from stagehand API');
        logger.info('Unable to fetch from stagehand API');
        return await processReviews(
            [],
            payload,
            'stagehand',
            `Stagehand Status: ${error.response?.status || 'N/A'} - ${error.response?.data?.error || error.message}`
        );
    }
}


async function fetchSOSDataFromAPI(checkId, payload, stateAbbreviation) {
    try {
        console.log('Try to fetch from API', payload);
        logger.info('Try to fetch from API', payload);
        const res = await axios.post(`http://54.224.20.169:8890/api/sos`, payload);
        //console.log('res-=--=', res?.data);
        let finaldata = res.data?.data;

        finaldata['companyId'] = checkId;
        finaldata['businessId'] = payload?.businessID;


        console.log('finaldata-=-', finaldata);
        return { status: true, message: 'Extract by stagehand API', data: [finaldata] };
    } catch (error) {
        if (error.response) {
            // ✅ API responded with an error
            console.error('API Error:', error.response.status, error.response.data);
            // If the API sends { error: "message" } → log that specifically
            if (error.response.data?.error) {
                console.error('Error Message:', error.response.data.error);
            }
        } else if (error.request) {
            // ✅ No response received from server
            console.error('No response from API:', error.request);
        } else {
            // ✅ Other unexpected errors
            console.error('Unexpected Error:', error.message);
        }
        return {
            status: false,
            message: `Stagehand Status: ${error.response?.status || 'N/A'} - ${error.response?.data?.error || error.message} `,
            data: []
        };
    }
}

function parseRatingToFive(input) {
    if (input == null) return null;
    const s = String(input).trim();

    // Map number words -> numeric
    const WORD_TO_NUM = {
        zero: 0, one: 1, two: 2, three: 3, four: 4, five: 5,
        six: 6, seven: 7, eight: 8, nine: 9, ten: 10
    };
    const toNum = (str) => {
        const t = String(str).trim().toLowerCase();
        if (t in WORD_TO_NUM) return WORD_TO_NUM[t];
        const n = parseFloat(t);
        return isFinite(n) ? n : NaN;
    };
    const scaleToFive = (val, base) =>
        isFinite(val) && isFinite(base) && base > 0
            ? Math.round(((val / base) * 5) * 10) / 10
            : null;

    // 1) "x out of y"  (digits OR words, with optional 'star(s)')
    {
        const m = s.match(/([\d.]+|[a-z]+)\s*(?:star[s]?)?\s*out\s*of\s*([\d.]+|[a-z]+)\s*(?:star[s]?)?/i);
        if (m) {
            const val = toNum(m[1]);
            const base = toNum(m[2]);
            const scaled = scaleToFive(val, base);
            if (scaled != null) return scaled;
        }
    }

    // 2) "x/y"
    {
        const m = s.match(/^\s*([\d.]+)\s*\/\s*([\d.]+)\s*$/);
        if (m) {
            const val = parseFloat(m[1]);
            const base = parseFloat(m[2]);
            const scaled = scaleToFive(val, base);
            if (scaled != null) return scaled;
        }
    }

    // 3) Percent, e.g., "80%"
    {
        const m = s.match(/^\s*([\d.]+)\s*%\s*$/);
        if (m) {
            const pct = parseFloat(m[1]);
            if (isFinite(pct)) return Math.round(((pct / 100) * 5) * 10) / 10;
        }
    }

    // 4) Stars as glyphs, e.g., "★★★★★" / "★★★☆☆"
    if (/★|☆/.test(s)) {
        const full = (s.match(/★/g) || []).length;
        const total = (s.match(/★|☆/g) || []).length || 5;
        return Math.round(((full / total) * 5) * 10) / 10;
    }

    // 5) "Rated X stars out of Y stars" (digits OR words)
    {
        const m = s.match(/rated\s+([\d.]+|[a-z]+)\s*star[s]?\s*out\s*of\s*([\d.]+|[a-z]+)\s*star[s]?/i);
        if (m) {
            const val = toNum(m[1]);
            const base = toNum(m[2]);
            const scaled = scaleToFive(val, base);
            if (scaled != null) return scaled;
        }
    }

    // 6) "X stars" / "X star" (digits OR words)
    {
        const m = s.match(/^\s*([\d.]+|[a-z]+)\s*star[s]?\s*$/i);
        if (m) {
            const val = toNum(m[1]);
            if (isFinite(val)) {
                // If clearly 0–5, use as-is; if ≤10, scale down
                if (val <= 5) return Math.round(val * 10) / 10;
                if (val <= 10) return Math.round(((val / 10) * 5) * 10) / 10;
            }
        }
    }

    // 7) Plain number
    {
        const num = parseFloat(s);
        if (isFinite(num)) {
            if (num <= 5) return Math.round(num * 10) / 10;
            if (num <= 10) return Math.round(((num / 10) * 5) * 10) / 10;
        }
    }

    return null; // unrecognized
}

function processReviews(reviewsparam, payload, scrapeSource, ApiErrorMessage = '') {
    console.log("inside process reviews", reviewsparam);
    logger.info("inside process reviews");
    let scraperText = '';
    try {
        let badReviewratingCount = 0;
        let incidentReason = '';

        let scraperTextTitleMissing = '';

        let reviews = JSON.parse(JSON.stringify(reviewsparam));

        const now = new Date();

        if (reviews.length > 0) {
            if (reviews[0].status !== undefined) {
                console.log("review with status fails..");
                logger.info("review with status fails..");
                scraperText = reviews[0].scraperMessage;
                reviews = [];
            } else {

                //console.log("reviews found", reviews);

                for (const review of reviews) {
                    const reviewDate = new Date(review.date);
                    const diffTime = Math.abs(now - reviewDate);
                    const diffDays = Math.ceil(diffTime / (1000 * 60 * 60 * 24));

                    const ratingStr = review?.ratings ?? review?.rating;
                    const normalized = parseRatingToFive(ratingStr);
                    if (normalized != null) {
                        review.ratings = normalized; // keep numeric (0–5)
                    }

                    if (review.ratings && parseInt(review.ratings) < 3 && diffDays < 60) {
                        badReviewratingCount++;
                    }
                    if (!review.date || isNaN(Date.parse(review.date))) {
                        scraperText = 'Some of reviews have no date';
                    }

                    if (payload?.source === 'play.google.com') {
                        review.title = '-';
                    } else if (review.title === undefined || review.title === null || review.title === '') {
                        scraperTextTitleMissing = 'Some of reviews have no title';
                    }

                }
                if (badReviewratingCount > 0) {
                    incidentReason = 'Recent review(s) have poor ratings';
                }
            }
        }

        if (scrapeSource === 'stagehand') {
            if (reviews?.length > 0) {
                scraperText += 'Stagehand successful;'
            } else {
                scraperText += `Stagehand failed (${ApiErrorMessage})`
            }

        } else {
            if (reviews?.length > 0) {
                scraperText += 'Node successful;'

            } else {
                scraperText += `Node failed`
            }
        }

        logger.info('scraperText=-=-', scraperText);
        return {
            data: reviews,
            status: true,
            checkId: payload.id,
            'scrapeSource': scrapeSource,
            incidentObj: {
                reason: incidentReason,
                status: badReviewratingCount === 0 ? "PASS" : "FAIL",
                when: new Date().toISOString(),
                checkId: payload.id,
                accountId: payload.accId || ""
            },
            url: payload.url,
            scraperMessage: `${scraperText};${scraperTextTitleMissing}`
        };
    } catch (error) {
        console.error("processReview error..", error);
        if (scrapeSource === 'stagehande') {
            scraperText += '\nUnable to fetch reviews using stagehand';
        }
        return {
            data: [],
            status: false,
            'scrapeSource': scrapeSource,
            checkId: payload.id,
            incidentObj: {
                reason: "",
                status: "PASS",
                when: new Date().toISOString(),
                checkId: payload.id
            },
            url: payload.url,
            scraperMessage: `error while processing reviews..${scraperText}`
        };
    }

}

async function insertReviews(data) {
    try {
        logger.info('inside insertreviews', data?.length);
        if (!Array.isArray(data) || data.length === 0) {
            return { data: [] };
        }

        // First attempt
        let response = await postReviewsOnce(data);
        //console.log('response-==-', response?.data);
        // Handle 401 → refresh session and retry once
        if (response.status === 401) {
            console.warn("Session expired/invalid. Refreshing sessionId and retrying once...");
            await ensureSessionId(true);         // force refresh
            response = await postReviewsOnce(data);
        }

        // Process response
        if (response.status === 200) {
            logger.info("API response:", response.data?.length);
            return response.data;
        }

        if (response.status === 400) {
            const errorData = response.data;
            console.warn("Partial or duplicate review insert:", errorData?.message);
            return {
                data: errorData?.data || [],
                message: errorData?.message,
                status: "partial",
            };
        }

        // Other statuses
        console.error("Unexpected review insert response:", response.status, response.data);
        return {
            data: [],
            error: response?.data?.message || `Unexpected status ${response.status}`,
            statusCode: response.status,
        };

    } catch (error) {
        // Network/timeout/other axios errors
        const payload = error?.response?.data || error?.message || String(error);
        console.error("Error inserting reviews:", payload);
        return { data: [], error: payload };
    }
}

async function insertAppLogs(data) {
    try {
        logger.info('insert log...', data);

        await logClient
            .db("applogs")
            .collection("scraper_url_log")
            .insertMany(data);


        //console.log(queryresp);
        return true
    } catch (error) {
        console.error('error==', error);
    }
}


async function insertIncident(sessionid, data, retry = true) {
    try {
        logger.info("Incident object", sessionid, JSON.stringify(data));

        const response = await axios.post(
            `${Host}/portend/v1/incidents`,
            [data],
            { headers: { 'sessionId': sessionid } }
        );

        logger.info("insertIncident API response:", response.data);
        return response.data;
    } catch (error) {
        const isAxios = axios.isAxiosError(error);
        const status = isAxios ? error.response?.status : null;

        // ✅ Refresh session and retry once on 401
        if (status === 401 && retry) {
            logger.warn("insertIncident got 401. Refreshing sessionId and retrying...");

            await ensureSessionId(true); // refresh global sessionId

            // use refreshed global sessionId (don’t reuse old one)
            return await insertIncident(sessionId, data, false);
        }

        const err = isAxios
            ? {
                message: error.message,
                code: error.code,
                status: error.response?.status,
                url: error.config?.url,
                method: error.config?.method,
            }
            : { message: String(error) };

        console.error("insertIncident API error:", JSON.stringify(err));
        return null;
    }
}


const updateExtractionDate = async (source) => {
    try {

        //  await client.connect();
        await client
            .db("collect")
            .collection("portend_scrapers")
            .updateOne(
                { 'source': source }, // filter by source
                {
                    $set: {
                        extractionDate: new Date().toISOString(), // set extractionDate
                    },
                }
            );


        //console.log(queryresp);
        return true
    } catch (error) {
        console.error('error==', error);
    }
};


const insertCompanyData = async (source, scraperRes) => {
    try {
        let collectionMap = {
            'crunchbase.com': 'crunchbaseCompany',
            'pitchbook.com': 'pitchbookCompany',
            'owler.com': 'owlerCompany',
            'finance.yahoo.com': 'yahooCompany',
            'PVTValuation.com': 'yahooPVTValuation',
            'builtwith.com': 'builtwith'   // ✅ added
        };

        // ✅ Select DB based on source
        const dbName = source === 'builtwith.com' 
            ? 'chromeplugin' 
            : 'datasets';

        const db = client.db(dbName);

        const collection = db.collection(collectionMap[source]);

        if (!collection) {
            console.error(`❌ No collection mapping found for source: ${source}`);
            return { data: [] };
        }

        const bulkOps = scraperRes.map((record) => ({
            updateOne: {
                filter: { companyId: record.companyId },
                update: {
                    $set: { ...record }
                },
                upsert: true,
            },
        }));

        if (bulkOps.length > 0) {
            const result = await collection.bulkWrite(bulkOps);
            logger.info("✅ Bulk write result:", result);
            return { data: [{}] };
        } else {
            logger.info("⚠️ No operations to execute.");
            return { data: [] };
        }

    } catch (error) {
        console.error('Company data insert error:', error);
        return { data: [] };
    }
};

const insertSOSData = async (source, scraperRes) => {
    try {

        // await client.connect();
        const db = client.db("collect"); // change if your DB name is different
        const collection = db.collection(`companies_${source?.toLowerCase()}`); // <-- replace with actual collection name

        const bulkOps = scraperRes.map((record) => ({
            updateOne: {
                filter: { companyId: record.companyId, businessId: record.businessId },
                update: {
                    $set: { ...record }
                },
                upsert: true,
            },
        }));

        if (bulkOps.length > 0) {
            const result = await collection.bulkWrite(bulkOps);
            logger.info("✅ Bulk write result:", result);
            return { data: [{}] };
        } else {
            logger.info("⚠️ No operations to execute.");
            return { data: [] };
        }


    } catch (error) {
        console.error('Company data insert error:', error);
        return { data: [] };
    }
};

const updateScraperDate = async (scraper) => {
    try {
        logger.info('updating date', scraper);
        //await client.connect();
        const db = client.db("collect");
        const result = await db.collection("sos_scrapers").updateOne(
            { name: scraper }, // filter
            {
                $set: {
                    date: new Date().toISOString()
                }
            }
        );

        logger.info("Matched:", result.matchedCount, "Modified:", result.modifiedCount);
        return true;
    } catch (error) {
        return true;
    }
}



const appleScraper = require("./Scrapers/Apple");
const trustpilotScraper = require("./Scrapers/Trustpilot");
const gartnerScraper = require("./Scrapers/Gartner");
const foursquareScraper = require("./Scrapers/Foursquare");
const glassdoorScraper = require("./Scrapers/Glassdoor");
const g2Scraper = require("./Scrapers/G2Crowd.js");
const capterraScraper = require("./Scrapers/Capterra.js");
const tripadvisorScraper = require("./Scrapers/Tripadvisor");
const healthgradesScraper = require("./Scrapers/Healthgrades");
const googlePlayScraper = require("./Scrapers/Googleplay");
const shopifyScraper = require("./Scrapers/Shopify");
const hubspotScraper = require("./Scrapers/Hubspot");
const softwareAdviceScraper = require("./Scrapers/SoftwareAdvice");
const realselfScraper = require("./Scrapers/Realself");
const amazonScraper = require("./Scrapers/Amazon");
const realPatientRatingsScraper = require("./Scrapers/Realpatientratings");
const googlereviewScraper = require("./Scrapers/Googlereview");
const bookingScraper = require("./Scrapers/Booking");
const expediaScraper = require("./Scrapers/Expedia");
const trustradiusScraper = require("./Scrapers/Trustradius");
const patientconnect365Scraper = require("./Scrapers/Patientconnect365");
const producthuntScraper = require("./Scrapers/Producthunt");
const bizprofileScraper = require("./Scrapers/bizprofile");
const { extractZocdocReviews } = require("./Scrapers/Zocdoc");
const { extractIndeedReviews } = require("./Scrapers/Indeed");
const { extractBBBReviews } = require("./Scrapers/BBB");
const { extractYelpReviews } = require("./Scrapers/Yelp");
const hotelsScraper = require("./Scrapers/Hotels");
const grubhubScraper = require("./Scrapers/Grubhub");
const doordashScraper = require("./Scrapers/Doordash");
const opentableScraper = require("./Scrapers/Opentable");
const zomatoScraper = require("./Scrapers/Zomato");
const getappScraper = require("./Scrapers/Getapp");
const goodFirmsScraper = require("./Scrapers/Goodfirms.js");
const walmartScraper = require("./Scrapers/Walmart");
const ratemdsScraper = require("./Scrapers/RateMDs");
const yellowpagesScraper = require("./Scrapers/Yellowpages");
const { extractOwlerCompanyData } = require("./Scrapers/Owler");
const { extractCrunchbaseData } = require("./Scrapers/CrunchBase");
const { extractPitchbookData } = require("./Scrapers/PitchBook");
const { extractBuiltWithData } = require("./Scrapers/builtwith.js");
const { extractYahooFinanceData } = require("./Scrapers/YahooFinance.js");
const { scrapeTexasCompanyData } = require("./Scrapers/TX.js");
const { scrapeNCCompanyData } = require("./Scrapers/NC.js");
const { scrapeWACompanyData } = require("./Scrapers/WA.js");
const { scrapeDECompanyData } = require("./Scrapers/DE.js");
const { scrapeKSCompanyData } = require("./Scrapers/KS.js");
const { scrapeKentuckyCompanyData } = require("./Scrapers/KY.js");
const { scrapeLouisianaCompanyData } = require("./Scrapers/LA.js");
const { scrapeIdahoCompanyData } = require("./Scrapers/ID.js");
const { scrapeCaliforniaCompanyData } = require("./Scrapers/CA.js");
const { scrapeMississippiCompanyData } = require("./Scrapers/MS.js");
const { scrapeUtahCompanyData } = require("./Scrapers/UT.js");
const { scrapeFLCompanyData } = require("./Scrapers/FL.js");
const { scrapeHawaiiCompanyData } = require("./Scrapers/HI.js");
const { scrapeMaineCompanyData } = require("./Scrapers/ME.js");
const { scrapeRhodeIslandCompanyData } = require("./Scrapers/RI.js");
const { scrapeNorthDakotaCompanyData } = require("./Scrapers/ND.js");
const { scrapeWestVirginiaCompanyData } = require("./Scrapers/WV.js");
const { scrapeColoradoCompanyData } = require("./Scrapers/CO.js");
const { scrapeWyomingCompanyData } = require("./Scrapers/WY.js");
const { scrapeMarylandCompanyData } = require("./Scrapers/MD.js");

module.exports = {
    scraperMap: {
        bizprofileScraper: {
            match: (url, htmlContent) => url.includes("bizprofile.net"),
            extractor: bizprofileScraper.extractBizProfileData,
        },
        booking: {
            match: (url, htmlContent) => url.includes("booking.com"),
            extractor: bookingScraper.extractBookingReviews,
        },
        trustpilot: {
            match: (url, htmlContent) => url.includes("trustpilot.com") && htmlContent !== '',
            extractor: trustpilotScraper.extractTrustpilotReviews,
        },
        expedia: {
            match: (url, htmlContent) => url.includes("expedia.") && htmlContent !== '',
            extractor: expediaScraper.extractExpediaReviews,
        },
        trustradius: {
            match: (url, htmlContent) => url.includes("trustradius.") && htmlContent !== '',
            extractor: trustradiusScraper.extractTrustRadiusReviews,
        },
        appleappstore: {
            match: (url, htmlContent) => url.includes("apple.") && htmlContent !== '',
            extractor: appleScraper.extractAppleReviews,
        },
        gartner: {
            match: (url, htmlContent) => url.includes("gartner.") && htmlContent !== '',
            extractor: gartnerScraper.extractGartnerReviews,
        },
        googlereview: {
            match: (url, htmlContent) => url.includes("google.") && htmlContent !== '',
            extractor: googlereviewScraper.extractGoogleReviews,
        },
        producthunt: {
            match: (url, htmlContent) => url.includes("producthunt.") && htmlContent !== '',
            extractor: producthuntScraper.extractProductHuntReviews,
        },
        patientconnect365: {
            match: (url, htmlContent) => url.includes("patientconnect365.") && htmlContent !== '',
            extractor: patientconnect365Scraper.extractPatientConnectReviews,
        },
        realpatientratings: {
            match: (url, htmlContent) => url.includes("realpatientratings.") && htmlContent !== '',
            extractor: realPatientRatingsScraper.extractRealPatientRatings,
        },
        amazonreview: {
            match: (url, htmlContent) => url.includes("amazon.") && htmlContent !== '',
            extractor: amazonScraper.extractAmazonReviews,
        },
        realself: {
            match: (url, htmlContent) => url.includes("realself.") && htmlContent !== '',
            extractor: realselfScraper.extractRealSelfReviews,
        },
        softwareadvice: {
            match: (url, htmlContent) => url.includes("softwareadvice.") && htmlContent !== '',
            extractor: softwareAdviceScraper.extractSoftwareAdviceReviews,
        },
        hubspotmarketplace: {
            match: (url, htmlContent) => url.includes("hubspot.") && htmlContent !== '',
            extractor: hubspotScraper.extractHubspotReviews,
        },
        shopifyappstore: {
            match: (url, htmlContent) => url.includes("shopify.") && htmlContent !== '',
            extractor: shopifyScraper.extractShopifyReviews,
        },
        googleplay: {
            match: (url, htmlContent) => url.includes("play.google.") && htmlContent !== '',
            extractor: googlePlayScraper.extractGooglePlayReviews,
        },
        healthgrades: {
            match: (url, htmlContent) => url.includes("healthgrades.") && htmlContent !== '',
            extractor: healthgradesScraper.extractHealthgradesReviews,
        },
        foursquare: {
            match: (url, htmlContent) => url.includes("foursquare.") && htmlContent !== '',
            extractor: foursquareScraper.extractFoursquareReviews,
        },
        glassdoor: {
            match: (url, htmlContent) => url.includes("glassdoor.") && htmlContent !== '',
            extractor: glassdoorScraper.extractGlassdoorReviews,
        },
        g2:{
            match: (url, htmlContent) => url.includes("g2.") && htmlContent !== '',
            extractor: g2Scraper.extractG2Reviews,
            
        },
        g2crowd:{
            match: (url, htmlContent) => url.includes("g2.") && htmlContent !== '',
            extractor: g2Scraper.extractG2Reviews,
            
        },
        capterra: {
            match: (url, htmlContent) => url.includes("capterra.") && htmlContent !== '',
            extractor: capterraScraper.extractCapterraReviews,
        },
        tripadvisor: {
            match: (url, htmlContent) => url.includes("tripadvisor.") && htmlContent !== '',
            extractor: tripadvisorScraper.extractTripadvisorReviews,
        },
        zocdoc: {
            match: (url, htmlContent) => url.includes("zocdoc.") && htmlContent !== '',
            extractor: extractZocdocReviews,
        },
        indeed: {
            match: (url, htmlContent) => url.includes("indeed.") && htmlContent !== '',
            extractor: extractIndeedReviews,
        },
        bbb: {
            match: (url, htmlContent) => url.includes("bbb.") && htmlContent !== '',
            extractor: extractBBBReviews,
        },
        yelp: {
            match: (url, htmlContent) => url.includes("yelp.") && htmlContent !== '',
            extractor: extractYelpReviews,
        },

        doordash: {
            match: (url, htmlContent) => url.includes("doordash.") && htmlContent !== '',
            extractor: doordashScraper.extractDoorDashReviews,
        },
        getapp: {
            match: (url, htmlContent) => url.includes("getapp.") && htmlContent !== '',
            extractor: getappScraper.extractGetAppReviews,
        },
        goodfirms: {
            match: (url, htmlContent) => url.includes("goodfirms.") && htmlContent !== '',
            extractor: goodFirmsScraper.extractGoodFirmsReviews,
        },
        grubhub: {
            match: (url, htmlContent) => url.includes("grubhub.") && htmlContent !== '',
            extractor: grubhubScraper.extractGrubhubReviews,
        },
        hotels: {
            match: (url, htmlContent) => url.includes("hotels.") && htmlContent !== '',
            extractor: hotelsScraper.extractHotelReviews,
        },
        opentable: {
            match: (url, htmlContent) => url.includes("opentable.") && htmlContent !== '',
            extractor: opentableScraper.extractOpenTableReviews,
        },
        ratemds: {
            match: (url, htmlContent) => url.includes("ratemds.") && htmlContent !== '',
            extractor: ratemdsScraper.extractRateMDsReviews,
        },
        walmart: {
            match: (url, htmlContent) => url.includes("walmart.") && htmlContent !== '',
            extractor: walmartScraper.extractWalmartReviews,
        },
        yellowpages: {
            match: (url, htmlContent) => url.includes("yellowpages.") && htmlContent !== '',
            extractor: yellowpagesScraper.extractYellowPagesReviews,
        },
        zomato: {
            match: (url, htmlContent) => url.includes("zomato.") && htmlContent !== '',
            extractor: zomatoScraper.extractZomatoReviews,
        },
        owler: {
            match: (url, htmlContent) => url.includes("owler.") && htmlContent !== '',
            extractor: extractOwlerCompanyData,
        },
        crunchbase: {
            match: (url, htmlContent) => url.includes("crunchbase.") && htmlContent !== '',
            extractor: extractCrunchbaseData,
        },
        builtwith: {
            match: (url, htmlContent) => url.includes("builtwith.") && htmlContent !== '',
            extractor: extractBuiltWithData,
        },
        pitchbook: {
            match: (url, htmlContent) => url.includes("pitchbook.") && htmlContent !== '',
            extractor: extractPitchbookData,
        },
        yahoofinance: {
            match: (url, htmlContent) => url.includes("finance.yahoo.") && htmlContent !== '',
            extractor: extractYahooFinanceData,
        },
        tx: {
            match: (url, htmlContent) => stateAbbreviation === "TX",
            extractor: scrapeTexasCompanyData,
        },
        nc: {
            match: (url, htmlContent) => stateAbbreviation === "NC",
            extractor: scrapeNCCompanyData,
        },
        wa: {
            match: (url, htmlContent) => stateAbbreviation === "WA",
            extractor: scrapeWACompanyData,
        },
        de: {
            match: (url, htmlContent) => stateAbbreviation === "DE",
            extractor: scrapeDECompanyData,
        },
        ks: {
            match: (url, htmlContent) => stateAbbreviation === "KS",
            extractor: scrapeKSCompanyData,
        },
        ky: {
            match: (url, htmlContent) => stateAbbreviation === "KY",
            extractor: scrapeKentuckyCompanyData,
        },
        la: {
            match: (url, htmlContent) => stateAbbreviation === "LA",
            extractor: scrapeLouisianaCompanyData,
        },
        id: {
            match: (url, htmlContent) => stateAbbreviation === "ID",
            extractor: scrapeIdahoCompanyData,
        },
        ca: {
            match: (url, htmlContent) => stateAbbreviation === "CA",
            extractor: scrapeCaliforniaCompanyData,
        },
        ms: {
            match: (url, htmlContent) => stateAbbreviation === "MS",
            extractor: scrapeMississippiCompanyData,
        },
        ut: {
            match: (url, htmlContent) => stateAbbreviation === "UT",
            extractor: scrapeUtahCompanyData,
        },
        fl: {
            match: (url, htmlContent) => stateAbbreviation === "FL",
            extractor: scrapeFLCompanyData,
        },
        hi: {
            match: (url, htmlContent) => stateAbbreviation === "HI",
            extractor: scrapeHawaiiCompanyData,
        },
        me: {
            match: (url, htmlContent) => stateAbbreviation === "ME",
            extractor: scrapeMaineCompanyData,
        },
        ri: {
            match: (url, htmlContent) => stateAbbreviation === "RI",
            extractor: scrapeRhodeIslandCompanyData,
        },
        nd: {
            match: (url, htmlContent) => stateAbbreviation === "ND",
            extractor: scrapeNorthDakotaCompanyData,
        },
        wv: {
            match: (url, htmlContent) => stateAbbreviation === "WV",
            extractor: scrapeWestVirginiaCompanyData,
        },
        co: {
            match: (url, htmlContent) => stateAbbreviation === "CO",
            extractor: scrapeColoradoCompanyData,
        },
        wy: {
            match: (url, htmlContent) => stateAbbreviation === "WY",
            extractor: scrapeWyomingCompanyData,
        },
        md: {
            match: (url, htmlContent) => stateAbbreviation === "MD",
            extractor: scrapeMarylandCompanyData,
        }
    }
};
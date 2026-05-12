const fs = require("fs");
const path = require("path");

const config = JSON.parse(
  fs.readFileSync(path.resolve('config', 'config.json'), 'utf8')
);

const StagehandConfig = {
  verbose: 1, // Verbosity level for logging: 0 = silent, 1 = info, 2 = all
  domSettleTimeoutMs: 30000, // Timeout for DOM to settle in milliseconds

  // LLM configuration
  modelName: "google/gemini-2.0-flash", // Name of the model to use
  modelClientOptions: {
    apiKey: config.SOS.GOOGLE_API_KEY,
  },

  // Browser configuration
  env: "LOCAL", // Environment to run in: LOCAL or BROWSERBASE
  apiKey: config.SOS.BROWSERBASE_API_KEY, // API key for authentication
  projectId: config.SOS.BROWSERBASE_PROJECT_ID, // Project identifier
  browserbaseSessionID: undefined, // Session ID for resuming Browserbase sessions
  browserbaseSessionCreateParams: {
    projectId: config.SOS.BROWSERBASE_PROJECT_ID,
    browserSettings: {
      blockAds: true,
      viewport: {
        width: 1024,
        height: 768,
      },
    },
  },
  localBrowserLaunchOptions: {
    viewport: {
      width: 1024,
      height: 768,
    },
  }, // Configuration options for the local browser
};

module.exports = StagehandConfig;

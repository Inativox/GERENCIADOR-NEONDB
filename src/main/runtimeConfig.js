const { app } = require('electron');
const path = require('path');
const { createPrivateConfig } = require('./privateConfig');
module.exports = createPrivateConfig({ app, projectRoot: path.resolve(__dirname, '../..') });

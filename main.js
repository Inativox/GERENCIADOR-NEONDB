const { app } = require('electron');

// Installed and portable versions share the same userData and checkpoints.
// Acquire the lock before loading any module that writes their configuration.
if (!app.requestSingleInstanceLock()) {
    app.quit();
} else {
    app.on('second-instance', () => {
        const state = require('./src/main/state');
        const window = state.mainWindow || state.loginWindow;
        if (!window || window.isDestroyed()) return;
        if (window.isMinimized()) window.restore();
        window.show();
        window.focus();
    });
    require('./src/main/index.js');
}

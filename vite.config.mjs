import { defineConfig } from 'vite';
import { fileURLToPath } from 'node:url';

export default defineConfig({
    define: { 'process.env.NODE_ENV': JSON.stringify('production') },
    build: {
        outDir: 'out/renderer',
        emptyOutDir: true,
        sourcemap: true,
        target: 'chrome120',
        lib: {
            entry: fileURLToPath(new URL('./src/renderer/react/main.tsx', import.meta.url)),
            name: 'BaseManagerColumns',
            formats: ['iife'],
            fileName: () => 'react.js',
            cssFileName: 'react',
        },
    },
});

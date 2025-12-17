import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'
import { resolve } from 'path'
import { writeFileSync, readFileSync } from 'fs'

// Plugin to copy index.html to 200.html for Render SPA fallback
const renderSpaPlugin = () => ({
  name: 'render-spa-fallback',
  closeBundle() {
    const indexPath = resolve(__dirname, 'dist/index.html')
    const fallbackPath = resolve(__dirname, 'dist/200.html')
    const content = readFileSync(indexPath, 'utf-8')
    writeFileSync(fallbackPath, content)
    console.log('Created 200.html for Render SPA fallback')
  }
})

// https://vite.dev/config/
export default defineConfig({
  plugins: [react(), renderSpaPlugin()],
})

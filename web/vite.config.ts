import react from '@vitejs/plugin-react'
import { defineConfig } from 'vite'

export default defineConfig({
  plugins: [react()],
  server: {
    // 本机开发时把 /api 转发给本机的后端（python -m pmdata.api.app），浏览器看来是同一个网址，不用管 CORS。
    // 8000 被 api 容器占着；宿主机另开一个端口试新接口时设 API_PROXY=http://127.0.0.1:8001
    proxy: { '/api': process.env.API_PROXY ?? 'http://127.0.0.1:8000' },
  },
})

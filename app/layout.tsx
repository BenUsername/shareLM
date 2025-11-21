import type { Metadata } from 'next'
import './globals.css'

export const metadata: Metadata = {
  title: 'ShareLM Dataset Analysis',
  description: 'Analysis dashboard for the ShareLM Hugging Face dataset',
}

export default function RootLayout({
  children,
}: {
  children: React.ReactNode
}) {
  return (
    <html lang="en">
      <body>{children}</body>
    </html>
  )
}

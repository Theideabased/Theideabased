import type { Metadata } from "next";
import { Instrument_Serif, Inter, JetBrains_Mono } from "next/font/google";
import "./globals.css";

const inter = Inter({
  subsets: ["latin"],
  variable: "--font-inter",
  display: "swap",
});

const instrumentSerif = Instrument_Serif({
  subsets: ["latin"],
  weight: "400",
  variable: "--font-instrument",
  display: "swap",
});

const jetBrainsMono = JetBrains_Mono({
  subsets: ["latin"],
  variable: "--font-jetbrains",
  display: "swap",
});

export const metadata: Metadata = {
  title: {
    default: "Seyi Ogunmusire — Senior Agentic AI Engineer",
    template: "%s — Seyi Ogunmusire",
  },
  description:
    "Senior agentic AI engineer building production agents, multi-agent workflows, autonomous tools, and the infrastructure that makes them reliable.",
  keywords: [
    "Seyi Ogunmusire",
    "senior agentic AI engineer",
    "AI agent engineer",
    "multi-agent systems",
    "agent orchestration",
    "autonomous agents",
    "LLM applications",
    "Kaggle expert",
    "MantaJobs",
  ],
  authors: [{ name: "Seyi Ogunmusire" }],
  openGraph: {
    type: "website",
    title: "Seyi Ogunmusire — Senior Agentic AI Engineer",
    description:
      "I engineer AI agents that move from reasoning to reliable action.",
    siteName: "Seyi Ogunmusire",
  },
  twitter: {
    card: "summary_large_image",
    title: "Seyi Ogunmusire — Senior Agentic AI Engineer",
    description:
      "Production agents, multi-agent systems, tool orchestration, and AI product engineering.",
  },
};

export default function RootLayout({ children }: Readonly<{ children: React.ReactNode }>) {
  return (
    <html lang="en" className="dark" suppressHydrationWarning>
      <body className={`${inter.variable} ${instrumentSerif.variable} ${jetBrainsMono.variable} antialiased`}>
        {children}
      </body>
    </html>
  );
}

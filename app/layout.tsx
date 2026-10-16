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
    default: "Seyi Ogunmusire — Software Engineer Building AI Solutions",
    template: "%s — Seyi Ogunmusire",
  },
  description:
    "Software engineer building production AI solutions, agentic systems, APIs, automation, data pipelines, and products that solve real problems.",
  keywords: [
    "Seyi Ogunmusire",
    "software engineer",
    "AI software engineer",
    "AI solutions engineer",
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
    title: "Seyi Ogunmusire — Software Engineer Building AI Solutions",
    description:
      "I build software that turns AI into useful solutions.",
    siteName: "Seyi Ogunmusire",
  },
  twitter: {
    card: "summary_large_image",
    title: "Seyi Ogunmusire — Software Engineer Building AI Solutions",
    description:
      "Production AI products, agentic systems, APIs, automation, and data engineering.",
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

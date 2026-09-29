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
    default: "Seyi Ogunmusire — AI Product Builder & Growth Operator",
    template: "%s — Seyi Ogunmusire",
  },
  description:
    "Seyi Ogunmusire builds AI products, automation systems, and growth engines around real human problems.",
  keywords: [
    "Seyi Ogunmusire",
    "AI product builder",
    "AI agents",
    "automation",
    "growth marketing",
    "Kaggle expert",
    "MantaJobs",
  ],
  authors: [{ name: "Seyi Ogunmusire" }],
  openGraph: {
    type: "website",
    title: "Seyi Ogunmusire — I build the product and the momentum.",
    description:
      "AI engineering, automation, data, sales, and growth—connected into useful products.",
    siteName: "Seyi Ogunmusire",
  },
  twitter: {
    card: "summary_large_image",
    title: "Seyi Ogunmusire — AI Product Builder & Growth Operator",
    description:
      "AI engineering, automation, data, sales, and growth—connected into useful products.",
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

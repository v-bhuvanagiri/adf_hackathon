import type { Metadata } from "next";
import { Inter as FontSans } from "next/font/google";
import "./globals.css";
import Sidebar from "@/components/ui/sidebar";
import { Header } from "@/components/Header";

import { cn } from "@/lib/utils";
export const metadata: Metadata = {
  title: "Sam's Sight",
  description: "AI Tool for efficient Data Handling.",
};

const fontSans = FontSans({
  subsets: ["latin"],
  variable: "--font-sans",
});

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="en">
      <body
        className={cn(
          "min-h-screen bg-background font-sans antialiased",
          fontSans.variable
        )}
      >
        {/* <div className="flex">
        

          <Sidebar />

          
          <main className="flex-grow">
            {children}
          </main>
        </div>  */}
      
          <main className="">{children}</main>
       
      </body>
    </html>
  );
}

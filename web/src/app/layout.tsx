import type { Metadata } from "next";
import "./globals.css";

export const metadata: Metadata = {
  title: "GenesisGo | Event Reliability Testing",
  description: "A local-first workspace for event reliability testing.",
};

export default function RootLayout({ children }: LayoutProps<"/">) {
  return (
    <html lang="en">
      <body>{children}</body>
    </html>
  );
}

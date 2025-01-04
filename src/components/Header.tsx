"use client";
import * as React from "react";
import Link from "next/link";
import { useRouter } from "next/navigation";

import { cn } from "@/lib/utils";
import { Separator } from "@/components/ui/separator";
import { Button, buttonVariants } from "@/components/ui/button";

import { IconLogout, IconSeparator } from "@/components/ui/icons";
import { Avatar, AvatarFallback, AvatarImage } from "@/components/ui/avatar";

import { CircleUserRound } from "lucide-react";
import { PT_Sans as FontSans } from "next/font/google";

const fontSans700 = FontSans({
  subsets: ["latin"],
  variable: "--font-sans",
  weight: "700",
});
export function Header() {
  const router = useRouter();
  const name = localStorage.getItem("name"); // Retrieve the user's role from local storage

  const handleLogout = () => {
    // Clear the authentication token from localStorage
    localStorage.removeItem("token");
    localStorage.removeItem("role");
    localStorage.removeItem("name");


    // Redirect to the login page
    router.push("/login");
  };

  return (
    <header className="fixed top-0 z-50 flex items-center justify-between w-full h-16 px-4 border-b shrink-0 bg-gradient-to-b from-background/10 via-background/50 to-background/80 backdrop-blur-xl">
      <div className="flex items-center gap-2">
        <img src="/eyes.png" className="w-8" />
        <div className={cn(" font-sans ", fontSans700.variable)}>
          Sam's Sight
        </div>
        <IconSeparator className="w-6 h-6 text-muted-foreground/50" />
        <div className="flex items-center justify-center gap-2">
          <div className="flex items-center justify-center text-xs font-medium uppercase rounded-full select-none h-7 w-7 shrink-0 bg-muted/50 text-muted-foreground">
            {name && name.charAt(0)}
          </div>
          <div className="text-[14px]">Hello, {name}</div>
        </div>
      </div>
      <div className="flex items-center gap-2">
        <button
          onClick={handleLogout}
          className="flex items-center justify-center p-2  hover:text-gray-700"
        >
          <IconLogout />
        </button>
      </div>
    </header>
  );
}

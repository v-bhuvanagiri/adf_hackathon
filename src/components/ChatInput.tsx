"use client";

import { useState, useRef } from "react";
import { Textarea } from "@/components/ui/textarea";
import { IconArrowElbow, IconPlus } from "@/components/ui/icons";
import { useRouter } from "next/navigation";
import { cn } from "@/lib/utils";
import { Button, buttonVariants } from "@/components/ui/button";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
  DialogTrigger,
} from "@/components/ui/dialog";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Toaster, toast } from "sonner";
export default function ChatInput({ onSubmit, isLoading , input, setInput}) {
  const [url, setUrl] = useState("");
  const [dialogOpen, setDialogOpen] = useState(false);
  const [role, setRole] = useState("");
  const formRef = useRef<HTMLFormElement>(null);
  const router = useRouter();
  const token = localStorage.getItem("token");
  const userRole = localStorage.getItem("role");

  const handleUrlChange = (e) => setUrl(e.target.value);
  const handleRoleChange = (e) => setRole(e.target.value);
  const handleInputChange = (e) => setInput(e.target.value);

  const handleSubmit = async (e) => {
    e.preventDefault();
    if (!input.trim()) return;
    await onSubmit(input);
    setInput("");
  };

  const handleUploadData = async (e) => {
    e.preventDefault();

    try {
      // Send the collected URL and role details to the backend
      const response = await fetch("http://localhost:5000/upload_data", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          Authorization: token,
        },
        body: JSON.stringify({ url, role }),
      });
      setUrl("");
      setRole("");
      if (response.status === 200) {
        setDialogOpen(false);
        toast.success("Knowledge Base Updated Successfully.");
      }
    } catch (error) {
      console.error("Error uploading data:", error);
    }
  };

  function handleKeyDown(e: React.KeyboardEvent<HTMLTextAreaElement>) {
    if (e.key === "Enter" && !e.shiftKey) {
      e.preventDefault();
      formRef.current?.requestSubmit();
    }
  }

  return (
    <>
      <div className="px-96 fixed inset-x-0 bottom-8 bg-gradient-to-b from-muted/10 from-10% to-muted/30 to-50% ">
        <div className="">
          <form ref={formRef} onSubmit={handleSubmit} className="">
            <div className="relative flex  w-full grow shadow-sm flex-col overflow-hidden bg-background px-8 rounded-md h-16 sm:border sm:px-12">
              <Dialog open={dialogOpen} onOpenChange={setDialogOpen}>
                <DialogTrigger asChild>
                  <Button
                    variant="outline"
                    className={cn(
                      buttonVariants({ size: "sm", variant: "outline" }),
                      "absolute left-0 top-4 h-8 w-8 rounded-full bg-background p-0 sm:left-4"
                    )}
                  >
                    <IconPlus />
                  </Button>
                </DialogTrigger>
                {userRole === "admin" ? (
                  <DialogContent className="sm:max-w-[425px]">
                    <DialogHeader>
                      <DialogTitle>Update Knowledge Base</DialogTitle>
                      <DialogDescription>
                        Please enter the data details and the role to update the
                        LLM's knowledge base.
                      </DialogDescription>
                    </DialogHeader>
                    <form onSubmit={handleUploadData}>
                      <div className="grid gap-4 py-4">
                        <div className="grid grid-cols-4 items-center gap-4">
                          <Label htmlFor="link" className="text-right">
                            Data Link
                          </Label>
                          <Input
                            id="url"
                            value={url}
                            onChange={handleUrlChange}
                            className="col-span-3"
                            placeholder="Link to your data..."
                          />
                        </div>
                        <div className="grid grid-cols-4 items-center gap-4">
                          <Label htmlFor="role" className="text-right">
                            Role
                          </Label>
                          <Input
                            id="role"
                            value={role}
                            onChange={handleRoleChange}
                            className="col-span-3"
                            placeholder="Role related to the data..."
                          />
                        </div>
                      </div>
                      <DialogFooter>
                        <Button type="submit">Save changes</Button>
                      </DialogFooter>
                    </form>
                  </DialogContent>
                ) : (
                  <DialogContent>
                    <DialogHeader>
                      <DialogTitle>Access Restricted</DialogTitle>
                      <DialogDescription>
                        Only admins can update the knowledge base.
                      </DialogDescription>
                    </DialogHeader>
                  </DialogContent>
                )}
              </Dialog>
              <Textarea
                className=" w-full resize-none bg-transparent px-4 py-[1.3rem]     focus-within:outline-none sm:text-sm"
                placeholder="Send a message."
                value={input}
                onChange={handleInputChange}
                onKeyDown={handleKeyDown}
                disabled={isLoading}
              />
              <Button
                type="submit"
                size="icon"
                disabled={!input || isLoading}
                className="absolute top-1/2 transform -translate-y-1/2 right-4  h-8 w-8"
              >
                <IconArrowElbow />
              </Button>
            </div>
          </form>
        </div>
      </div>
      <Toaster richColors position="top-right" />
    </>
  );
}

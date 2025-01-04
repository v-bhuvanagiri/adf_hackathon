import { Card, CardHeader } from "@/components/ui/card";
import { Message as MessageType } from "ai";
import { Bot, User } from "lucide-react";
import { IconOpenAI, IconUser } from '@/components/ui/icons'
import ReactMarkdown from 'react-markdown';

export default function Message({ message }: { message: MessageType }) {
  const { role, content } = message;
  if (role === "assistant") {
    return (
      <div className="flex flex-col gap-3 p-6 whitespace-pre-wrap">
        <div className="flex items-center gap-2">
          <img src="/eyes.png" className="w-4" />
          Sight:
        </div>
        <ReactMarkdown>{content}</ReactMarkdown>
      </div>
    );
  }
  return (
    <div className="flex flex-col gap-3 p-6 whitespace-pre-wrap">
        <div className="flex items-center gap-2">
          <IconUser />
          {content}
        </div>
    
    </div>
  );
}

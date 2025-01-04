import { UseChatHelpers } from "ai/react";

import { Button } from "@/components/ui/button";
import { IconArrowRight } from "@/components/ui/icons";

const exampleMessages = [
  {
    heading: "Explain me the leave policy.",
    message: `How do I submit a leave request? Can you guide me through the process? Help me with this`,
  },
  {
    heading: "What is the stock level across each product line?",
    message: `What is the stock level across each product line`,
  },
  {
    heading: "Who are my best suppliers?",
    message: `Give me the best performing vendor for each warehouse with their average lead time.`,
  },
];

export function EmptyScreen({ setInput }: Pick<UseChatHelpers, "setInput">) {
  return (
    <div className="mx-auto max-w-2xl px-4 h-screen flex justify-center items-center">
      <div className="rounded-lg border bg-background p-8">
        <h1 className="mb-2 text-lg font-semibold">Welcome to Sam’s Sight!</h1>
        <p className="mb-2 leading-normal text-muted-foreground">
          I’m here to help you make your work easier and faster. You no longer
          have to sit through long Excel sheets and analyze data. Not a techie?
          No problem! I’m here to assist you with your tasks by providing useful
          insights. And if you’re a new joiner, I’ve got you covered! Any small
          or embarrassing doubt you have, just ask me, and I’ll get it
          clarified.
        </p>
        <p className="leading-normal text-muted-foreground">
          You can start a conversation here or try the following examples:
        </p>
        <div className="mt-4 flex flex-col items-start space-y-2">
          {exampleMessages.map((message, index) => (
            <Button
              key={index}
              variant="link"
              className="h-auto p-0 text-base"
              onClick={() => setInput(message.message)}
            >
              <IconArrowRight className="mr-2 text-muted-foreground" />
              {message.heading}
            </Button>
          ))}
        </div>
      </div>
    </div>
  );
}

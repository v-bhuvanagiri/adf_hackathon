"use client";
import { useState, useEffect } from "react";
import { useRouter } from "next/navigation";
import MessagesList from "@/components/MessagesList";
import ChatInput from "@/components/ChatInput";
import { Header } from "@/components/Header";
import { EmptyScreen } from "@/components/EmptyScreen";



export default function Home() {
  const [messages, setMessages] = useState([]);
  const [isLoading, setIsLoading] = useState(false);
  const [input, setInput] = useState("");

  const router = useRouter();
 


  useEffect(() => {
    const token = localStorage.getItem("token");
    if (!token) {
      router.push("/login");
    }
  }, []);

  const handleSubmit = async (input) => {
    const newUserMessage = { role: "user", content: input };
    setMessages((prevMessages) => [...prevMessages, newUserMessage]);
    setIsLoading(true);

    try {
      const token = localStorage.getItem("token");
      const role = localStorage.getItem("role");  // Retrieve the user's role from local storage
      const response = await fetch("http://localhost:5000/chat", {
            method: "POST",
            headers: {
                "Content-Type": "application/json",
                Authorization: token,
            },
            body: JSON.stringify({ messages: [...messages, newUserMessage], role }),  // Pass the role to the backend
        });
      if (!response.ok) throw new Error("Network response was not ok");

      const contentType = response.headers.get("Content-Type");

      if (contentType.includes("application/json")) {
        const data = await response.json();
        if (data.x && data.y) {
          // Associate graphData with the last message (the assistant's response)
          setMessages((prevMessages) =>
            prevMessages.map((msg, index) =>
              index === prevMessages.length - 1
                ? { ...msg, graphData: data }
                : msg
            )
          );
        }
      } else {
        const reader = response.body.getReader();
        const decoder = new TextDecoder();
        let assistantResponse = "";

        setMessages((prevMessages) => [
          ...prevMessages,
          { role: "assistant", content: "" },
        ]);

        while (true) {
          const { done, value } = await reader.read();
          if (done) break;
          const chunk = decoder.decode(value);
          assistantResponse += chunk;
          setMessages((prevMessages) => [
            ...prevMessages.slice(0, -1),
            { role: "assistant", content: assistantResponse },
          ]);
        }
      }
    } catch (error) {
      console.error("Error:", error);
    } finally {
      setIsLoading(false);
    }
  };

  return (
    <div>
      <Header />
      <main className="w-full h-screen bg-muted">
        <div className="container h-full w-full flex flex-col py-8">
          {messages.length ? (
          <>
            <MessagesList messages={messages} isLoading={isLoading} />
          </>
        ) : (
          <EmptyScreen setInput={setInput} />
        )}
          <ChatInput onSubmit={handleSubmit} isLoading={isLoading}  input={input} setInput={setInput}/>
        </div>
      </main>
    </div>
  );
}

"use client";
import Message from "./Messges";
import Plot from "react-plotly.js"; // Plotly for graph rendering
import { Separator } from "@/components/ui/separator";
import { Skeleton } from "@/components/ui/skeleton";

export default function MessagesList({ messages, isLoading }) {
  console.log(isLoading);
  return (
    <div className="flex-1 px-52 py-16 overflow-y-auto scrollbar-hide">
      {messages.map((message, index) => (
        <div key={index}>
          <Message message={message} />
          {message.graphData && (
            <>
              <Plot
                data={[
                  {
                    x: message.graphData.x,
                    y:
                      message.graphData.y.length > 0
                        ? message.graphData.y
                        : undefined,
                    type:
                      message.graphData.type === "bar"
                        ? "bar"
                        : message.graphData.type === "line"
                        ? "scatter"
                        : message.graphData.type === "pie"
                        ? "pie"
                        : undefined,
                    mode:
                      message.graphData.type === "line"
                        ? "lines+markers"
                        : undefined,
                    labels:
                      message.graphData.type === "pie"
                        ? message.graphData.x
                        : undefined,
                    values:
                      message.graphData.type === "pie"
                        ? message.graphData.y
                        : undefined,
                    marker: { color: "blue" },
                  },
                ]}
                layout={{ title: "Generated Graph" }}
              />
              <Separator />
            </>
          )}

          <Separator />
          {isLoading && index === messages.length - 1 ? (
            <div className="flex flex-col gap-3 p-6 animate-pulse">
              <div className="flex items-center gap-2">
                <div className="w-4 h-4 bg-gray-300 rounded-full"></div>
                <div className="h-4 w-24 bg-gray-300 rounded"></div>
              </div>
              <div className="space-y-2">
                <div className="h-4 bg-gray-300 rounded w-full"></div>
                <div className="h-4 bg-gray-300 rounded w-5/6"></div>
                <div className="h-4 bg-gray-300 rounded w-4/6"></div>
              </div>
            </div>
          ) : (
            <></>
          )}
        </div>
      ))}
    </div>
  );
}

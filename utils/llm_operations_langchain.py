from pydantic import BaseModel, Field
from typing import Literal
from langchain_core.output_parsers import PydanticOutputParser, JsonOutputParser
from langchain_core.prompts.prompt import PromptTemplate
from langchain_openai import AzureChatOpenAI
from openai import AzureOpenAI
import os
from dotenv import load_dotenv
load_dotenv()

client = AzureOpenAI(
        azure_endpoint="https://voicecast-gpt-france.openai.azure.com/",
        api_key=os.environ["OPENAI_API_KEY"],
        api_version="2025-01-01-preview",
    )

def create_completion(content, model="gpt-4.1"):
    system_msg = "You are a helpful assistant who know English and German Language"
    response = client.chat.completions.create(model=model, 
        messages=[
            {"role": "system", "content": system_msg},
            {"role": "user", "content": content},
        ],
        temperature = 0
    )
    return response.choices[0].message.content

def create_batch_yaml_sample(prompt, sender_id):
    
    return {
    "custom_id": sender_id,
    "method": "POST",
    "url": "/v1/chat/completions",
    "body": {
        "model": "gpt-4.1-batch",
        "messages": [
            {"role": "system", "content": "You are a helpful assistant who know English and German Language"},
            {"role": "user", "content": prompt}
            ]
        }
    }

llm = AzureChatOpenAI(model="gpt-4o", 
                      temperature = 0.,  
                      api_key = os.environ["OPENAI_API_KEY"], 
                      azure_endpoint = "https://voicecast-gpt-france.openai.azure.com/",
                      api_version="2024-02-15-preview"
                     )

prompt_exm  = """

You are evaluating a conversation between company employee and internal IT service desk chatbot. Chatbot is called CSD chatbot and it is supposed to help employees with their workplace related issues, be it HW, SW, account, password or similar issue. Chatbot can provide either self-service support with troubleshooting and resolution of the issue, navigate to proper manual via hyperlink, trigger ticket creation or involvement of human agent.

Your goal is to evaluate the interaction from the **user’s perspective**, as if you were the user. Assess how the user likely felt during the interaction, particularly how satisfied, frustrated, or neutral they were based on the responses they received.

Use the following criteria to assess the conversation quality:

---

Evaluation Criteria:
1.	User Sentiment:
	•	Look for signs of frustration, repetition, escalation, or unresolved issues.
	•	Be cautious about users who initially seem neutral but express dissatisfaction near the end.
	•	If the sentiment is ambiguous or not clearly positive/negative, return “Unclear”.
2.	Task Completion:
	•	Did the bot resolve the user’s request or guide them to a solution?
	•	Or did the conversation end without a clear resolution?
3.	Clarity of Understanding:
	•	Did the bot understand the intent clearly?
	•	Was the user forced to rephrase or correct the bot?
4.	Tone and Sentiment of Bot:
	•	Was the tone helpful, cold, overly generic, or repetitive?
	•	Did it acknowledge the user’s problem empathetically?
5.	User Experience Flow:
	•	Was the interaction smooth and intuitive?
	•	Did the bot offer clear next steps or help links?
6.	Escalation or Confusion:
	•	Did the bot offer escalation to human support when needed?
	•	Did the bot loop the user or confuse them further?
7. Unclear Conversations:
   Assign the **“Unclear”** label **only** when one or more of the following apply:
    • User message is vague, contextless, or disconnected from previous messages.
    • There are **loops or resets in the conversation** without progress or clarity.
    • User intent keeps shifting without confirmation, or user gives up or changes topic suddenly.

If a message is UnClear then dont consider any other evaluation criteria. Return only unclear.

Strict Instruction:
	•	Always penalize for repetition of bot prompts i.e. bot did not understand the user.
	•	If the user has to click the same button twice or is asked to rephrase repeatedly, subtract marks.
	•	Do not give high scores if the conversation ends without clear confirmation that the user’s issue is resolved.
	•	If the user’s sentiment is unclear and you cannot infer it, return "UnClear" with a score of 0 in the final remarks.
    •   User intent keeps shifting without confirmation, or user gives up or changes topic suddenly. In This return "UNCLEAR"

---

Input format:
CONVERSATION:
{chat_transcript}

Output format:
{format_instruction}

"""

class chat_evaluator(BaseModel):
    score: Literal[0, 1, 2, 3, 4, 5] = Field(description = """
    Satisfaction Score: [0 to 5]  
        0 = Unclear, Unclear conversation, confusion, unresolved, user intent misunderstood or lost.
        1 = Very dissatisfied  
        2 = Dissatisfied  
        3 = Neutral / Mixed  
        4 = Satisfied  
        5 = Very satisfied
    """)
    reasoning: str = Field(description = """
    Short and crisp pointers for the score awarded.
    Low scores means more pointers in the negative sections.
    High scores means more pointers in the positive sections.
    The Summary should have 2 sections.
    Positive:
    •	What was positive pointers in the chat, considering the evaluation criteria. 
    Negative:
    •	What was negative pointers in the chat, considering the evaluation criteria. If there is no negative pointer, then return "No negative experience"
    """)
    satisfaction_label: Literal["Unclear", "Very Dissatisfied", 
        "Dissatisfied", "Neutral", "Satisfied", 
        "Very Satisfied"] = Field(description = """
        The field is a mapping of the score 
        Mapping for the label is dependent on the score and is mentioned below
        0 = Unclear, Unclear conversation, confusion, unresolved, user intent misunderstood or lost.
        1 = Very dissatisfied  
        2 = Dissatisfied  
        3 = Neutral / Mixed  
        4 = Satisfied  
        5 = Very satisfied
        """)


parser = PydanticOutputParser(pydantic_object = chat_evaluator)


prompt = PromptTemplate(input_variables = ["chat_transcript"], 
                        partial_variables = {"format_instruction": parser.get_format_instructions()},
                        template = prompt_exm
                       )

chain = prompt | llm | JsonOutputParser()

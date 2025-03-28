import openai
from groundx import GroundX

class ChatbotBackend:
    def __init__(self):
        self.groundx = GroundX(
            api_key="39535690-c81b-4528-908c-2d7725e03fa5"
        )
        openai.api_key = "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"
        self.completion_model = "gpt-4"
        self.instruction = "You are a helpful virtual assistant that answers questions using the content below. Your task is to create detailed answers to the questions by combining your understanding of the world with the content provided below. Do not share links."

    def get_response(self, query, doc_id=17068):
        try:
            content_response = self.groundx.search.content(
                id=doc_id,
                query=query
            )
            results = content_response.search
            llm_text = results.text

            completion = openai.ChatCompletion.create(
                model=self.completion_model,
                messages=[
                    {
                        "role": "system",
                        "content": f"{self.instruction}\n===\n{llm_text}\n==="
                    },
                    {"role": "user", "content": query},
                ],
            )

            return {
                "response": completion.choices[0].message.content,
                "score": results.score
            }
        except Exception as e:
            return {"error": str(e)}

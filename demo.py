import openai
from groundx import GroundX

query = "summarize the contents of Fake-Resume.pdf"

groundx = GroundX(
  api_key="39535690-c81b-4528-908c-2d7725e03fa5"
)
openai.api_key = "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"

# Fix model name - using gpt-4 instead of gpt-4o
completion_model = "gpt-4"
instruction = "You are a helpful virtual assistant that answers questions using the content below. Your task is to create detailed answers to the questions by combining your understanding of the world with the content provided below. Do not share links."

try:
    content_response = groundx.search.content(
        id=17068, 
        query="What is interests in that resume?"
    )
    results = content_response.search
    llm_text = results.text

    completion = openai.ChatCompletion.create(
        model=completion_model,
        messages=[
            {
                "role": "system",
                "content": f"{instruction}\n===\n{llm_text}\n==="
            },
            {"role": "user", "content": query},
        ],
    )

    print(f"""
QUERY
{query}
SCORE
[{results.score:.2f}]
RESULT
{completion.choices[0].message.content}
""".strip())  # Added strip() to remove extra whitespace

except Exception as e:
    print(f"An error occurred: {str(e)}")




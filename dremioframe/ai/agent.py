import os
import glob
from typing import Optional, List, Union
from langchain_core.tools import tool
from langchain_core.language_models import BaseChatModel
from langchain_openai import ChatOpenAI
from langchain_anthropic import ChatAnthropic
from langchain_google_genai import ChatGoogleGenerativeAI
from langgraph.prebuilt import create_react_agent

@tool
def list_documentation() -> List[str]:
    """Lists all available documentation files in the docs directory."""
    # Try to find docs dir
    possible_paths = [
        os.path.join(os.getcwd(), "docs"),
        os.path.join(os.path.dirname(__file__), "../../docs"),
    ]
    
    docs_path = None
    for p in possible_paths:
        if os.path.exists(p):
            docs_path = p
            break
    
    if not docs_path:
        return ["Error: Documentation directory not found."]

    files = glob.glob(os.path.join(docs_path, "**/*.md"), recursive=True)
    return [os.path.relpath(f, docs_path) for f in files]

@tool
def read_documentation(file_path: str) -> str:
    """Reads the content of a specific documentation file."""
    possible_paths = [
        os.path.join(os.getcwd(), "docs"),
        os.path.join(os.path.dirname(__file__), "../../docs"),
    ]
    
    docs_path = None
    for p in possible_paths:
        if os.path.exists(p):
            docs_path = p
            break
            
    if not docs_path:
        return "Error: Documentation directory not found."

    full_path = os.path.join(docs_path, file_path)
    if not os.path.exists(full_path):
        return f"Error: File {file_path} not found."
        
    with open(full_path, "r") as f:
        return f.read()

@tool
def search_dremio_docs(query: str) -> List[str]:
    """
    Searches native Dremio documentation in the dremiodocs directory.
    Returns a list of filenames that might be relevant.
    """
    possible_paths = [
        os.path.join(os.getcwd(), "dremiodocs"),
        os.path.join(os.path.dirname(__file__), "../../dremiodocs"),
    ]
    
    docs_path = None
    for p in possible_paths:
        if os.path.exists(p):
            docs_path = p
            break
            
    if not docs_path:
        return ["Error: Dremio documentation directory not found."]
        
    # Simple search: find files containing the query string (case-insensitive)
    matches = []
    for root, _, files in os.walk(docs_path):
        for file in files:
            if file.endswith(".md"):
                full_path = os.path.join(root, file)
                try:
                    with open(full_path, "r", errors="ignore") as f:
                        content = f.read()
                        if query.lower() in content.lower():
                            matches.append(os.path.relpath(full_path, docs_path))
                except Exception:
                    continue
    return matches[:5] # Return top 5 matches

@tool
def read_dremio_doc(file_path: str) -> str:
    """Reads the content of a specific Dremio documentation file."""
    possible_paths = [
        os.path.join(os.getcwd(), "dremiodocs"),
        os.path.join(os.path.dirname(__file__), "../../dremiodocs"),
    ]
    
    docs_path = None
    for p in possible_paths:
        if os.path.exists(p):
            docs_path = p
            break
            
    if not docs_path:
        return "Error: Dremio documentation directory not found."

    full_path = os.path.join(docs_path, file_path)
    if not os.path.exists(full_path):
        return f"Error: File {file_path} not found."
        
    with open(full_path, "r") as f:
        return f.read()

class DremioAgent:
    def __init__(self, model: str = "gpt-4o", api_key: Optional[str] = None, llm: Optional[BaseChatModel] = None):
        self.model_name = model
        self.api_key = api_key
        self.llm = llm or self._initialize_llm()
        self.tools = [list_documentation, read_documentation, search_dremio_docs, read_dremio_doc]
        self.agent = self._initialize_agent()

    def _initialize_llm(self):
        if "gpt" in self.model_name:
            api_key = self.api_key or os.environ.get("OPENAI_API_KEY")
            if not api_key:
                raise ValueError("OPENAI_API_KEY not found.")
            return ChatOpenAI(model=self.model_name, api_key=api_key, temperature=0)
        elif "claude" in self.model_name:
            api_key = self.api_key or os.environ.get("ANTHROPIC_API_KEY")
            if not api_key:
                raise ValueError("ANTHROPIC_API_KEY not found.")
            return ChatAnthropic(model=self.model_name, api_key=api_key, temperature=0)
        elif "gemini" in self.model_name:
            api_key = self.api_key or os.environ.get("GOOGLE_API_KEY")
            if not api_key:
                raise ValueError("GOOGLE_API_KEY not found.")
            return ChatGoogleGenerativeAI(model=self.model_name, google_api_key=api_key, temperature=0)
        else:
            api_key = self.api_key or os.environ.get("OPENAI_API_KEY")
            if api_key:
                return ChatOpenAI(model="gpt-4o", api_key=api_key, temperature=0)
            raise ValueError(f"Unsupported model or missing API key for {self.model_name}")

    def _initialize_agent(self):
        system_message = (
            "You are an expert Dremio developer assistant. Your goal is to generate Python scripts using the `dremioframe` library based on user requests. "
            "You have access to the library's documentation via tools. "
            "Always check the documentation if you are unsure about specific API usage. "
            "You also have access to native Dremio documentation via `search_dremio_docs` and `read_dremio_doc` if you need to understand underlying concepts or SQL functions. "
            "When generating code, ensure it is complete, runnable, and includes comments about required environment variables (DREMIO_PAT, DREMIO_PROJECT_ID). "
            "The output should be ONLY the python code block, or the code itself if saving to file."
        )
        return create_react_agent(self.llm, self.tools, prompt=system_message)

    def generate_script(self, prompt: str, output_file: Optional[str] = None) -> str:
        """
        Generates a dremioframe script based on the prompt.
        If output_file is provided, writes the code to the file.
        """
        response = self.agent.invoke({"messages": [("user", prompt)]})
        # LangGraph returns state, output is in messages[-1].content
        output = response["messages"][-1].content
        
        # Extract code block if present
        if "```python" in output:
            code = output.split("```python")[1].split("```")[0].strip()
        elif "```" in output:
            code = output.split("```")[1].split("```")[0].strip()
        else:
            code = output

        if output_file:
            with open(output_file, "w") as f:
                f.write(code)
            return f"Script generated and saved to {output_file}"
        
        return code

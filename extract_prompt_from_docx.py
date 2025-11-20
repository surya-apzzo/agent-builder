#!/usr/bin/env python3
"""
Extract text from DOCX file and format it for JSON request body.
This script helps convert DOCX prompt files to the prompt_text field format.
"""

import sys
import json
import re
from pathlib import Path

try:
    from docx import Document
except ImportError:
    print("Error: python-docx is required. Install it with: pip install python-docx")
    sys.exit(1)


def extract_text_from_docx(docx_path: str) -> str:
    """
    Extract all text from a DOCX file, preserving paragraph structure.
    
    Args:
        docx_path: Path to the DOCX file
        
    Returns:
        Text content with newlines preserved
    """
    try:
        doc = Document(docx_path)
        text_parts = []
        
        for paragraph in doc.paragraphs:
            if paragraph.text.strip():  # Skip empty paragraphs
                text_parts.append(paragraph.text.strip())
        
        # Join paragraphs with double newlines (common markdown format)
        text = "\n\n".join(text_parts)
        
        return text
    except Exception as e:
        print(f"Error reading DOCX file: {e}")
        sys.exit(1)


def format_for_json(text: str, escape_newlines: bool = True) -> str:
    """
    Format text for JSON request body.
    
    Args:
        text: Raw text content
        escape_newlines: If True, escape newlines as \n (for single-line JSON)
                        If False, keep actual newlines (for multi-line JSON)
    
    Returns:
        Formatted text ready for JSON
    """
    if escape_newlines:
        # Escape newlines, quotes, and backslashes for JSON
        text = text.replace("\\", "\\\\")  # Escape backslashes first
        text = text.replace('"', '\\"')    # Escape double quotes
        text = text.replace('\n', '\\n')   # Escape newlines
        text = text.replace('\r', '')      # Remove carriage returns
        text = text.replace('\t', '\\t')   # Escape tabs
    else:
        # Just escape quotes and backslashes (for multi-line JSON strings)
        text = text.replace("\\", "\\\\")
        text = text.replace('"', '\\"')
        text = text.replace('\r', '')
    
    return text


def main():
    if len(sys.argv) < 2:
        print("Usage: python extract_prompt_from_docx.py <path_to_docx> [--json] [--pretty]")
        print("\nOptions:")
        print("  --json     Output as complete JSON request body")
        print("  --pretty   Pretty print the JSON")
        print("\nExamples:")
        print("  python extract_prompt_from_docx.py prompt.docx")
        print("  python extract_prompt_from_docx.py prompt.docx --json")
        print("  python extract_prompt_from_docx.py prompt.docx --json --pretty")
        sys.exit(1)
    
    docx_path = sys.argv[1]
    output_json = "--json" in sys.argv
    pretty = "--pretty" in sys.argv
    
    if not Path(docx_path).exists():
        print(f"Error: File not found: {docx_path}")
        sys.exit(1)
    
    # Extract text
    print(f"📄 Extracting text from: {docx_path}")
    text = extract_text_from_docx(docx_path)
    
    if output_json:
        # Format for JSON (escape newlines)
        escaped_text = format_for_json(text, escape_newlines=True)
        
        # Create JSON request body
        json_body = {
            "prompt_text": text  # Use unescaped text - json.dumps will handle escaping
        }
        
        if pretty:
            output = json.dumps(json_body, indent=4, ensure_ascii=False)
        else:
            output = json.dumps(json_body, ensure_ascii=False)
        
        print("\n" + "="*80)
        print("📋 JSON Request Body (prompt_text field):")
        print("="*80)
        print(output)
        print("\n" + "="*80)
        print("💡 Copy the 'prompt_text' value and paste it into your onboarding request")
        print("="*80)
    else:
        # Just show the extracted text
        print("\n" + "="*80)
        print("📝 Extracted Text:")
        print("="*80)
        print(text)
        print("\n" + "="*80)
        print("💡 To format for JSON, run with --json flag")
        print("="*80)
    
    # Also show escaped version for manual copy-paste
    escaped_text = format_for_json(text, escape_newlines=True)
    print("\n" + "="*80)
    print("📋 Escaped Text (for manual JSON insertion):")
    print("="*80)
    print(escaped_text)
    print("="*80)


if __name__ == "__main__":
    main()




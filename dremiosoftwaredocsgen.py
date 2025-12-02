#!/usr/bin/env python3
"""
Generate a single PDF from all markdown documentation files.

This script:
1. Recursively finds all .md files in the docs/ directory
2. Combines them into a single markdown file with proper headers
3. Converts the combined markdown to PDF using markdown2 and weasyprint

Requirements:
    pip install markdown2 weasyprint
"""

import os
import glob
from pathlib import Path
from datetime import datetime

try:
    import markdown2
    from reportlab.lib.pagesizes import letter
    from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
    from reportlab.lib.units import inch
    from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, PageBreak, Preformatted, Table, TableStyle
    from reportlab.lib import colors
    from reportlab.lib.enums import TA_LEFT, TA_CENTER
    from html.parser import HTMLParser
except ImportError:
    print("Error: Required packages not installed.")
    print("Please run: pip install markdown2 reportlab")
    exit(1)


def find_markdown_files(docs_dir="dremiodocs/dremio-software"):
    """Find all markdown files in the docs directory."""
    md_files = []
    for root, dirs, files in os.walk(docs_dir):
        for file in files:
            if file.endswith('.md'):
                md_files.append(os.path.join(root, file))
    
    # Sort files for consistent ordering
    md_files.sort()
    return md_files


def read_markdown_file(filepath):
    """Read a markdown file and return its content."""
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            return f.read()
    except Exception as e:
        print(f"Warning: Could not read {filepath}: {e}")
        return ""


def combine_markdown_files(md_files, docs_dir="docs"):
    """Combine all markdown files into a single markdown string."""
    combined = []
    
    # Add title page
    combined.append("# DremioFrame Documentation")
    combined.append(f"\nGenerated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    combined.append("---\n")
    
    # Add table of contents
    combined.append("## Table of Contents\n")
    for filepath in md_files:
        rel_path = os.path.relpath(filepath, docs_dir)
        section_name = rel_path.replace('.md', '').replace('/', ' > ')
        combined.append(f"- {section_name}")
    combined.append("\n---\n")
    
    # Add each file's content
    for filepath in md_files:
        rel_path = os.path.relpath(filepath, docs_dir)
        section_name = rel_path.replace('.md', '').replace('/', ' > ')
        
        combined.append(f"\n\n# {section_name}\n")
        combined.append(f"*Source: `{rel_path}`*\n")
        combined.append("---\n")
        
        content = read_markdown_file(filepath)
        if content:
            combined.append(content)
        
        combined.append("\n\n---\n")
    
    return '\n'.join(combined)


class HTMLToReportLab(HTMLParser):
    """Simple HTML parser to convert HTML to ReportLab elements."""
    
    def __init__(self, styles):
        super().__init__()
        self.styles = styles
        self.elements = []
        self.current_text = []
        self.in_pre = False
        self.in_h1 = False
        self.in_h2 = False
        self.in_h3 = False
        self.in_p = False
        self.in_code = False
        
    def handle_starttag(self, tag, attrs):
        if tag == 'h1':
            self.in_h1 = True
            self.current_text = []
        elif tag == 'h2':
            self.in_h2 = True
            self.current_text = []
        elif tag == 'h3':
            self.in_h3 = True
            self.current_text = []
        elif tag == 'p':
            self.in_p = True
            self.current_text = []
        elif tag == 'pre':
            self.in_pre = True
            self.current_text = []
        elif tag == 'code' and not self.in_pre:
            self.in_code = True
        elif tag == 'hr':
            self.elements.append(Spacer(1, 0.2*inch))
            
    def handle_endtag(self, tag):
        if tag == 'h1' and self.in_h1:
            text = ''.join(self.current_text).strip()
            if text:
                self.elements.append(PageBreak())
                self.elements.append(Paragraph(text, self.styles['CustomHeading1']))
                self.elements.append(Spacer(1, 0.2*inch))
            self.in_h1 = False
            self.current_text = []
        elif tag == 'h2' and self.in_h2:
            text = ''.join(self.current_text).strip()
            if text:
                self.elements.append(Paragraph(text, self.styles['CustomHeading2']))
                self.elements.append(Spacer(1, 0.15*inch))
            self.in_h2 = False
            self.current_text = []
        elif tag == 'h3' and self.in_h3:
            text = ''.join(self.current_text).strip()
            if text:
                self.elements.append(Paragraph(text, self.styles['CustomHeading3']))
                self.elements.append(Spacer(1, 0.1*inch))
            self.in_h3 = False
            self.current_text = []
        elif tag == 'p' and self.in_p:
            text = ''.join(self.current_text).strip()
            if text:
                self.elements.append(Paragraph(text, self.styles['BodyText']))
                self.elements.append(Spacer(1, 0.1*inch))
            self.in_p = False
            self.current_text = []
        elif tag == 'pre' and self.in_pre:
            text = ''.join(self.current_text)
            if text:
                self.elements.append(Preformatted(text, self.styles['CustomCode']))
                self.elements.append(Spacer(1, 0.15*inch))
            self.in_pre = False
            self.current_text = []
        elif tag == 'code' and self.in_code:
            self.in_code = False
            
    def handle_data(self, data):
        if self.in_h1 or self.in_h2 or self.in_h3 or self.in_p or self.in_pre:
            self.current_text.append(data)


def markdown_to_pdf(markdown_content, output_file="dremioframe_docs.pdf"):
    """Convert markdown content to PDF using ReportLab."""
    
    # Ensure output directory exists
    output_dir = os.path.dirname(output_file)
    if output_dir and not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    # Convert markdown to HTML
    html_content = markdown2.markdown(
        markdown_content,
        extras=[
            'fenced-code-blocks',
            'tables',
            'header-ids',
            'code-friendly',
            'break-on-newline'
        ]
    )
    
    # Create PDF document
    print(f"Generating PDF: {output_file}")
    doc = SimpleDocTemplate(
        output_file,
        pagesize=letter,
        rightMargin=72,
        leftMargin=72,
        topMargin=72,
        bottomMargin=72
    )
    
    # Define styles
    styles = getSampleStyleSheet()
    
    # Customize styles
    styles.add(ParagraphStyle(
        name='CustomHeading1',
        parent=styles['Heading1'],
        fontSize=18,
        textColor=colors.HexColor('#2c3e50'),
        spaceAfter=12,
        spaceBefore=12,
        borderWidth=2,
        borderColor=colors.HexColor('#3498db'),
        borderPadding=5,
    ))
    
    styles.add(ParagraphStyle(
        name='CustomHeading2',
        parent=styles['Heading2'],
        fontSize=14,
        textColor=colors.HexColor('#34495e'),
        spaceAfter=10,
        spaceBefore=10,
    ))
    
    styles.add(ParagraphStyle(
        name='CustomHeading3',
        parent=styles['Heading3'],
        fontSize=12,
        textColor=colors.HexColor('#7f8c8d'),
        spaceAfter=8,
        spaceBefore=8,
    ))
    
    styles.add(ParagraphStyle(
        name='CustomCode',
        parent=styles['Code'],
        fontSize=9,
        leftIndent=20,
        rightIndent=20,
        backColor=colors.HexColor('#f8f8f8'),
        borderWidth=1,
        borderColor=colors.HexColor('#dddddd'),
        borderPadding=10,
    ))
    
    # Parse HTML and convert to ReportLab elements
    parser = HTMLToReportLab(styles)
    parser.feed(html_content)
    
    # Build PDF
    doc.build(parser.elements)
    print(f"✓ PDF generated successfully: {output_file}")


def main():
    """Main function to generate the documentation PDF."""
    docs_dir = "docs"
    output_file = "dremiosoftwaredocs.pdf"
    
    # Check if docs directory exists
    if not os.path.exists(docs_dir):
        print(f"Error: '{docs_dir}' directory not found.")
        exit(1)
    
    print("Finding markdown files...")
    md_files = find_markdown_files(docs_dir)
    
    if not md_files:
        print(f"No markdown files found in '{docs_dir}' directory.")
        exit(1)
    
    print(f"Found {len(md_files)} markdown files")
    
    print("Combining markdown files...")
    combined_markdown = combine_markdown_files(md_files, docs_dir)
    
    print("Converting to PDF...")
    markdown_to_pdf(combined_markdown, output_file)
    
    print(f"\n✓ Complete! Documentation saved to: {output_file}")
    print(f"  Total files processed: {len(md_files)}")
    print(f"  File size: {os.path.getsize(output_file) / 1024 / 1024:.2f} MB")


if __name__ == "__main__":
    main()

import sys
import zipfile
import xml.etree.ElementTree as ET


def docx_to_text(docx_path):
    try:
        with zipfile.ZipFile(docx_path) as z:
            xml_content = z.read("word/document.xml")
            root = ET.fromstring(xml_content)

            # XML namespaces
            ns = {"w": "http://schemas.openxmlformats.org/wordprocessingml/2006/main"}

            # Find all paragraph elements
            paragraphs = []
            for elem in root.iter():
                if elem.tag.endswith("p"):
                    # Gather all text in this paragraph
                    texts = [t.text for t in elem.findall(".//w:t", ns) if t.text]
                    if texts:
                        paragraphs.append("".join(texts))
                    else:
                        paragraphs.append("")
                elif elem.tag.endswith("tbl"):
                    # Process table cells
                    for row in elem.findall(".//w:tr", ns):
                        row_text = []
                        for cell in row.findall(".//w:tc", ns):
                            cell_text = "".join(
                                [t.text for t in cell.findall(".//w:t", ns) if t.text]
                            )
                            row_text.append(cell_text)
                        paragraphs.append(" | ".join(row_text))

            return "\n".join(paragraphs)
    except Exception as e:
        return f"Error reading {docx_path}: {e}"


if __name__ == "__main__":
    import os

    for doc in ["apis.docx", "backend_requirements.docx"]:
        path = os.path.join("..", doc)  # parent folder
        if not os.path.exists(path):
            path = doc
        print(f"=== {doc} ===")
        text = docx_to_text(path)
        with open(f"{doc}.txt", "w", encoding="utf-8") as f:
            f.write(text)
        print(f"Saved {doc}.txt")

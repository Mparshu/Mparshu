from pdf2docx import Converter

# Path to the input PDF and output Word document
pdf_path = "/mnt/data/CGI offer letter.pdf"
docx_path = "/mnt/data/CGI_offer_letter.docx"

# Convert PDF to Word document
cv = Converter(pdf_path)
cv.convert(docx_path, start=0, end=None)
cv.close()

docx_path


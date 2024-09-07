from PyPDF2 import PdfReader, PdfWriter
from io import BytesIO
from reportlab.pdfgen import canvas
from reportlab.lib.pagesizes import letter

# Load the original PDF
reader = PdfReader("/mnt/data/CGI offer letter.pdf")
writer = PdfWriter()

# Replace text on each page
for page_num in range(len(reader.pages)):
    page = reader.pages[page_num]
    text = page.extract_text()

    # Replace the target string
    updated_text = text.replace("Pavan Vijayrao Sawai", "New Things Summer")

    # Create a new PDF with the updated text
    packet = BytesIO()
    can = canvas.Canvas(packet, pagesize=letter)
    can.drawString(10, 750, updated_text)
    can.save()

    # Move to the beginning of the StringIO buffer
    packet.seek(0)
    new_pdf = PdfReader(packet)
    new_page = new_pdf.pages[0]

    # Combine the new page with the original one
    page.merge_page(new_page)
    writer.add_page(page)

# Save the modified PDF to a new file
output_pdf_path = "/mnt/data/CGI_offer_letter_edited.pdf"
with open(output_pdf_path, "wb") as output_file:
    writer.write(output_file)

output_pdf_path


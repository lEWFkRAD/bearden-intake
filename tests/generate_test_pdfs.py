"""Generate realistic test PDFs for the Bearden Document Intake Platform.

Creates sample tax documents and bank statements in data/test_documents/.
These are synthetic — no real PII — but structured like the real thing.
"""
import os
import sys
from pathlib import Path
from fpdf import FPDF

OUT_DIR = Path(__file__).parent.parent / "data" / "test_documents"
OUT_DIR.mkdir(parents=True, exist_ok=True)


# ─── Helpers ────────────────────────────────────────────────────────────────

def add_box(pdf, x, y, w, h, label, value, label_size=7, value_size=10):
    """Draw a labeled box with a value inside, mimicking IRS form layout."""
    pdf.set_draw_color(0, 0, 0)
    pdf.rect(x, y, w, h)
    pdf.set_font("Helvetica", "", label_size)
    pdf.set_xy(x + 1, y + 1)
    pdf.cell(w - 2, 4, label, ln=0)
    pdf.set_font("Courier", "B", value_size)
    pdf.set_xy(x + 2, y + 6)
    pdf.cell(w - 4, h - 8, value, ln=0, align="R")


# ─── W-2 ────────────────────────────────────────────────────────────────────

def create_w2():
    pdf = FPDF()
    pdf.add_page()

    # Header
    pdf.set_font("Helvetica", "B", 14)
    pdf.cell(0, 8, "Form W-2   Wage and Tax Statement   2024", ln=True, align="C")
    pdf.set_font("Helvetica", "", 8)
    pdf.cell(0, 5, "Department of the Treasury - Internal Revenue Service", ln=True, align="C")
    pdf.ln(5)

    # Employee info
    pdf.set_font("Helvetica", "", 8)
    pdf.cell(0, 4, "a  Employee's social security number", ln=True)
    pdf.set_font("Courier", "B", 11)
    pdf.cell(0, 6, "***-**-4521", ln=True)
    pdf.ln(2)

    pdf.set_font("Helvetica", "", 8)
    pdf.cell(0, 4, "e  Employee's first name and initial    Last name", ln=True)
    pdf.set_font("Courier", "B", 11)
    pdf.cell(0, 6, "JEFFREY A WATTS", ln=True)
    pdf.ln(2)

    pdf.set_font("Helvetica", "", 8)
    pdf.cell(0, 4, "f  Employee's address and ZIP code", ln=True)
    pdf.set_font("Courier", "", 9)
    pdf.cell(0, 5, "123 BROAD STREET", ln=True)
    pdf.cell(0, 5, "ROME, GA 30161", ln=True)
    pdf.ln(2)

    pdf.set_font("Helvetica", "", 8)
    pdf.cell(0, 4, "c  Employer's name, address, and ZIP code", ln=True)
    pdf.set_font("Courier", "", 9)
    pdf.cell(0, 5, "NORTH GEORGIA MANUFACTURING LLC", ln=True)
    pdf.cell(0, 5, "456 INDUSTRIAL BLVD", ln=True)
    pdf.cell(0, 5, "ROME, GA 30161", ln=True)
    pdf.ln(2)

    pdf.set_font("Helvetica", "", 8)
    pdf.cell(0, 4, "b  Employer identification number (EIN)", ln=True)
    pdf.set_font("Courier", "B", 11)
    pdf.cell(0, 6, "58-1234567", ln=True)
    pdf.ln(5)

    # Boxes - Left column
    y_start = 105
    col1_x = 10
    col2_x = 105
    bw = 85
    bh = 18
    gap = 20

    add_box(pdf, col1_x, y_start, bw, bh, "1  Wages, tips, other compensation", "78,450.00")
    add_box(pdf, col2_x, y_start, bw, bh, "2  Federal income tax withheld", "12,680.00")

    add_box(pdf, col1_x, y_start + gap, bw, bh, "3  Social security wages", "78,450.00")
    add_box(pdf, col2_x, y_start + gap, bw, bh, "4  Social security tax withheld", "4,863.90")

    add_box(pdf, col1_x, y_start + gap*2, bw, bh, "5  Medicare wages and tips", "78,450.00")
    add_box(pdf, col2_x, y_start + gap*2, bw, bh, "6  Medicare tax withheld", "1,137.53")

    add_box(pdf, col1_x, y_start + gap*3, bw, bh, "7  Social security tips", "0.00")
    add_box(pdf, col2_x, y_start + gap*3, bw, bh, "8  Allocated tips", "0.00")

    add_box(pdf, col1_x, y_start + gap*4, bw, bh, "10  Dependent care benefits", "0.00")
    add_box(pdf, col2_x, y_start + gap*4, bw, bh, "11  Nonqualified plans", "0.00")

    add_box(pdf, col1_x, y_start + gap*5, bw, bh, "12a  Code / Amount", "D    5,500.00")
    add_box(pdf, col2_x, y_start + gap*5, bw, bh, "13  Statutory / Retire / 3rd party", "X Retire")

    add_box(pdf, col1_x, y_start + gap*6, bw, bh, "14  Other", "")
    add_box(pdf, col2_x, y_start + gap*6, bw, bh, "15  State  GA    Employer state ID", "58-1234567")

    add_box(pdf, col1_x, y_start + gap*7, bw, bh, "16  State wages, tips, etc.", "78,450.00")
    add_box(pdf, col2_x, y_start + gap*7, bw, bh, "17  State income tax", "4,315.00")

    path = OUT_DIR / "W2_North_Georgia_Mfg_2024.pdf"
    pdf.output(str(path))
    print(f"  Created: {path.name}")
    return path


# ─── 1099-INT ───────────────────────────────────────────────────────────────

def create_1099_int():
    pdf = FPDF()
    pdf.add_page()

    # Header
    pdf.set_font("Helvetica", "B", 14)
    pdf.cell(0, 8, "Form 1099-INT   Interest Income   2024", ln=True, align="C")
    pdf.set_font("Helvetica", "", 8)
    pdf.cell(0, 5, "Copy B for Recipient", ln=True, align="C")
    pdf.ln(5)

    # Payer info
    pdf.set_font("Helvetica", "", 8)
    pdf.cell(0, 4, "PAYER'S name, street address, city, state, ZIP code, and telephone no.", ln=True)
    pdf.set_font("Courier", "", 9)
    pdf.cell(0, 5, "FIRST NATIONAL BANK OF NORTH GEORGIA", ln=True)
    pdf.cell(0, 5, "200 BROAD STREET", ln=True)
    pdf.cell(0, 5, "ROME, GA 30161    (706) 555-0100", ln=True)
    pdf.ln(2)

    pdf.set_font("Helvetica", "", 8)
    pdf.cell(0, 4, "PAYER'S TIN", ln=True)
    pdf.set_font("Courier", "B", 11)
    pdf.cell(0, 6, "58-7654321", ln=True)
    pdf.ln(2)

    pdf.set_font("Helvetica", "", 8)
    pdf.cell(0, 4, "RECIPIENT'S TIN", ln=True)
    pdf.set_font("Courier", "B", 11)
    pdf.cell(0, 6, "***-**-4521", ln=True)
    pdf.ln(2)

    pdf.set_font("Helvetica", "", 8)
    pdf.cell(0, 4, "RECIPIENT'S name", ln=True)
    pdf.set_font("Courier", "B", 10)
    pdf.cell(0, 5, "JEFFREY A WATTS", ln=True)
    pdf.ln(5)

    # Boxes
    y = 85
    bw = 85
    bh = 18
    gap = 20

    add_box(pdf, 10, y, bw, bh, "1  Interest income", "2,847.63")
    add_box(pdf, 105, y, bw, bh, "2  Early withdrawal penalty", "0.00")

    add_box(pdf, 10, y + gap, bw, bh, "3  Interest on U.S. Savings Bonds and Treas. obligations", "0.00")
    add_box(pdf, 105, y + gap, bw, bh, "4  Federal income tax withheld", "0.00")

    add_box(pdf, 10, y + gap*2, bw, bh, "5  Investment expenses", "0.00")
    add_box(pdf, 105, y + gap*2, bw, bh, "6  Foreign tax paid", "0.00")

    add_box(pdf, 10, y + gap*3, bw, bh, "8  Tax-exempt interest", "1,250.00")
    add_box(pdf, 105, y + gap*3, bw, bh, "9  Specified private activity bond interest", "0.00")

    path = OUT_DIR / "1099INT_First_National_Bank_2024.pdf"
    pdf.output(str(path))
    print(f"  Created: {path.name}")
    return path


# ─── 1099-DIV ───────────────────────────────────────────────────────────────

def create_1099_div():
    pdf = FPDF()
    pdf.add_page()

    pdf.set_font("Helvetica", "B", 14)
    pdf.cell(0, 8, "Form 1099-DIV   Dividends and Distributions   2024", ln=True, align="C")
    pdf.set_font("Helvetica", "", 8)
    pdf.cell(0, 5, "Copy B for Recipient", ln=True, align="C")
    pdf.ln(5)

    pdf.set_font("Helvetica", "", 8)
    pdf.cell(0, 4, "PAYER'S name, street address, city, state, ZIP code", ln=True)
    pdf.set_font("Courier", "", 9)
    pdf.cell(0, 5, "VANGUARD GROUP INC", ln=True)
    pdf.cell(0, 5, "PO BOX 982901", ln=True)
    pdf.cell(0, 5, "EL PASO, TX 79998-2901", ln=True)
    pdf.ln(2)

    pdf.set_font("Helvetica", "", 8)
    pdf.cell(0, 4, "PAYER'S TIN", ln=True)
    pdf.set_font("Courier", "B", 11)
    pdf.cell(0, 6, "23-1945930", ln=True)
    pdf.ln(2)

    pdf.set_font("Helvetica", "", 8)
    pdf.cell(0, 4, "RECIPIENT'S TIN", ln=True)
    pdf.set_font("Courier", "B", 11)
    pdf.cell(0, 6, "***-**-4521", ln=True)
    pdf.ln(2)

    pdf.set_font("Helvetica", "", 8)
    pdf.cell(0, 4, "RECIPIENT'S name", ln=True)
    pdf.set_font("Courier", "B", 10)
    pdf.cell(0, 5, "JEFFREY A WATTS", ln=True)
    pdf.ln(5)

    y = 85
    bw = 85
    bh = 18
    gap = 20

    add_box(pdf, 10, y, bw, bh, "1a  Total ordinary dividends", "4,231.50")
    add_box(pdf, 105, y, bw, bh, "1b  Qualified dividends", "3,892.10")

    add_box(pdf, 10, y + gap, bw, bh, "2a  Total capital gain distr.", "1,567.25")
    add_box(pdf, 105, y + gap, bw, bh, "2b  Unrecap. Sec. 1250 gain", "0.00")

    add_box(pdf, 10, y + gap*2, bw, bh, "3  Nondividend distributions", "0.00")
    add_box(pdf, 105, y + gap*2, bw, bh, "4  Federal income tax withheld", "0.00")

    add_box(pdf, 10, y + gap*3, bw, bh, "5  Section 199A dividends", "0.00")
    add_box(pdf, 105, y + gap*3, bw, bh, "6  Investment expenses", "0.00")

    add_box(pdf, 10, y + gap*4, bw, bh, "7  Foreign tax paid", "127.84")
    add_box(pdf, 105, y + gap*4, bw, bh, "8  Foreign country", "VARIOUS")

    path = OUT_DIR / "1099DIV_Vanguard_2024.pdf"
    pdf.output(str(path))
    print(f"  Created: {path.name}")
    return path


# ─── Bank Statement ─────────────────────────────────────────────────────────

def create_bank_statement():
    pdf = FPDF()
    pdf.add_page()

    # Header
    pdf.set_font("Helvetica", "B", 16)
    pdf.cell(0, 10, "First National Bank of North Georgia", ln=True, align="C")
    pdf.set_font("Helvetica", "", 10)
    pdf.cell(0, 6, "200 Broad Street  |  Rome, GA 30161  |  (706) 555-0100", ln=True, align="C")
    pdf.ln(3)

    pdf.set_draw_color(0, 0, 0)
    pdf.line(10, pdf.get_y(), 200, pdf.get_y())
    pdf.ln(3)

    pdf.set_font("Helvetica", "B", 12)
    pdf.cell(0, 7, "Business Checking Account Statement", ln=True)
    pdf.set_font("Helvetica", "", 9)
    pdf.cell(0, 5, "Statement Period:  January 1, 2024 - January 31, 2024", ln=True)
    pdf.cell(0, 5, "Account Number:  ****4589", ln=True)
    pdf.cell(0, 5, "Account Holder:  BEARDEN ACCOUNTING FIRM PC", ln=True)
    pdf.ln(5)

    # Summary
    pdf.set_font("Helvetica", "B", 10)
    pdf.cell(0, 7, "Account Summary", ln=True)
    pdf.set_font("Helvetica", "", 9)

    summary = [
        ("Beginning Balance (01/01/2024)", "45,230.18"),
        ("Total Deposits and Credits (8)", "32,750.00"),
        ("Total Withdrawals and Debits (23)", "28,415.67"),
        ("Ending Balance (01/31/2024)", "49,564.51"),
    ]
    for label, val in summary:
        pdf.cell(130, 5, "  " + label, border=0)
        pdf.cell(40, 5, val, border=0, align="R", ln=True)
    pdf.ln(5)

    # Transactions
    pdf.set_font("Helvetica", "B", 10)
    pdf.cell(0, 7, "Transaction Detail", ln=True)

    # Header row
    pdf.set_font("Helvetica", "B", 8)
    pdf.set_fill_color(230, 230, 230)
    pdf.cell(22, 6, "Date", border=1, fill=True)
    pdf.cell(80, 6, "Description", border=1, fill=True)
    pdf.cell(28, 6, "Debit", border=1, fill=True, align="R")
    pdf.cell(28, 6, "Credit", border=1, fill=True, align="R")
    pdf.cell(30, 6, "Balance", border=1, fill=True, align="R", ln=True)

    pdf.set_font("Courier", "", 7)
    txns = [
        ("01/02", "DEPOSIT - CLIENT PAYMENT JOHNSON CONST", "", "4,500.00", "49,730.18"),
        ("01/03", "ACH DEBIT - GA POWER COMPANY", "287.45", "", "49,442.73"),
        ("01/03", "ACH DEBIT - AT&T BUSINESS SERVICES", "189.99", "", "49,252.74"),
        ("01/05", "CHECK #4521 - OFFICE DEPOT", "342.18", "", "48,910.56"),
        ("01/05", "DEPOSIT - CLIENT PAYMENT SMITH FAMILY", "", "3,250.00", "52,160.56"),
        ("01/08", "ACH DEBIT - QUICKBOOKS SUBSCRIPTION", "85.00", "", "52,075.56"),
        ("01/08", "ACH DEBIT - RENT PAYMENT 123 BROAD ST", "2,800.00", "", "49,275.56"),
        ("01/10", "DEPOSIT - CLIENT PAYMENT ROME DENTAL", "", "2,500.00", "51,775.56"),
        ("01/10", "CHECK #4522 - STAPLES BUSINESS", "156.34", "", "51,619.22"),
        ("01/12", "ACH DEBIT - BLUE CROSS BLUE SHIELD", "1,850.00", "", "49,769.22"),
        ("01/15", "DEPOSIT - CLIENT PAYMENT FLOYD MED CTR", "", "8,500.00", "58,269.22"),
        ("01/15", "PAYROLL - JEFFREY WATTS", "3,750.00", "", "54,519.22"),
        ("01/15", "PAYROLL - SUSAN BEARDEN", "4,200.00", "", "50,319.22"),
        ("01/15", "PAYROLL - CHARLES BEARDEN", "5,100.00", "", "45,219.22"),
        ("01/17", "ACH DEBIT - THOMSON REUTERS", "425.00", "", "44,794.22"),
        ("01/18", "DEPOSIT - CLIENT PAYMENT CAVE SPRING AG", "", "3,500.00", "48,294.22"),
        ("01/19", "CHECK #4523 - FEDEX SHIPPING", "87.50", "", "48,206.72"),
        ("01/22", "DEPOSIT - CLIENT PAYMENT BERRY COLLEGE", "", "5,500.00", "53,706.72"),
        ("01/22", "ACH DEBIT - ADOBE CREATIVE SUITE", "59.99", "", "53,646.73"),
        ("01/24", "ACH DEBIT - VERIZON WIRELESS", "245.67", "", "53,401.06"),
        ("01/25", "DEPOSIT - CLIENT PAYMENT WATSON FARMS", "", "5,000.00", "58,401.06"),
        ("01/25", "CHECK #4524 - CITY OF ROME UTILITIES", "178.55", "", "58,222.51"),
        ("01/28", "ACH DEBIT - IRS EFTPS TAX PAYMENT Q4", "6,200.00", "", "52,022.51"),
        ("01/29", "ACH DEBIT - GA DOR STATE TAX PAYMENT", "2,458.00", "", "49,564.51"),
    ]

    for date, desc, debit, credit, bal in txns:
        pdf.cell(22, 5, date, border="LR")
        pdf.cell(80, 5, desc[:45], border="LR")
        pdf.cell(28, 5, debit, border="LR", align="R")
        pdf.cell(28, 5, credit, border="LR", align="R")
        pdf.cell(30, 5, bal, border="LR", align="R", ln=True)

    # Bottom border
    pdf.cell(188, 0, "", border="T", ln=True)

    path = OUT_DIR / "Bank_Statement_Jan_2024.pdf"
    pdf.output(str(path))
    print(f"  Created: {path.name}")
    return path


# ─── Multi-page K-1 ─────────────────────────────────────────────────────────

def create_k1():
    pdf = FPDF()

    # Page 1 - Main K-1
    pdf.add_page()
    pdf.set_font("Helvetica", "B", 13)
    pdf.cell(0, 8, "Schedule K-1 (Form 1065)   2024", ln=True, align="C")
    pdf.set_font("Helvetica", "", 8)
    pdf.cell(0, 5, "Partner's Share of Income, Deductions, Credits, etc.", ln=True, align="C")
    pdf.ln(5)

    # Part I - Partnership info
    pdf.set_font("Helvetica", "B", 9)
    pdf.cell(0, 6, "Part I    Information About the Partnership", ln=True)
    pdf.set_font("Helvetica", "", 8)
    pdf.cell(0, 4, "A  Partnership's employer identification number", ln=True)
    pdf.set_font("Courier", "B", 10)
    pdf.cell(0, 5, "58-9876543", ln=True)
    pdf.set_font("Helvetica", "", 8)
    pdf.cell(0, 4, "B  Partnership's name, address, city, state, and ZIP code", ln=True)
    pdf.set_font("Courier", "", 9)
    pdf.cell(0, 5, "COOSA VALLEY REAL ESTATE PARTNERS LP", ln=True)
    pdf.cell(0, 5, "789 TURNER MCCALL BLVD", ln=True)
    pdf.cell(0, 5, "ROME, GA 30161", ln=True)
    pdf.ln(3)

    # Part II - Partner info
    pdf.set_font("Helvetica", "B", 9)
    pdf.cell(0, 6, "Part II   Information About the Partner", ln=True)
    pdf.set_font("Helvetica", "", 8)
    pdf.cell(0, 4, "E  Partner's SSN or TIN", ln=True)
    pdf.set_font("Courier", "B", 10)
    pdf.cell(0, 5, "***-**-4521", ln=True)
    pdf.set_font("Helvetica", "", 8)
    pdf.cell(0, 4, "F  Partner's name, address, city, state, and ZIP code", ln=True)
    pdf.set_font("Courier", "", 9)
    pdf.cell(0, 5, "JEFFREY A WATTS", ln=True)
    pdf.cell(0, 5, "123 BROAD STREET", ln=True)
    pdf.cell(0, 5, "ROME, GA 30161", ln=True)
    pdf.ln(3)

    pdf.set_font("Helvetica", "", 8)
    pdf.cell(0, 4, "G  General partner or LLC member-manager", ln=True)
    pdf.cell(0, 4, "H1 Profit %: 15.000    H2 Loss %: 15.000    Capital %: 15.000", ln=True)
    pdf.ln(3)

    # Part III - Partner's share
    pdf.set_font("Helvetica", "B", 9)
    pdf.cell(0, 6, "Part III  Partner's Share of Current Year Income, Deductions, Credits", ln=True)

    y = 120
    bw = 85
    bh = 16
    gap = 18

    add_box(pdf, 10, y, bw, bh, "1  Ordinary business income (loss)", "12,340.00")
    add_box(pdf, 105, y, bw, bh, "2  Net rental real estate income (loss)", "8,750.00")

    add_box(pdf, 10, y + gap, bw, bh, "3  Other net rental income (loss)", "0.00")
    add_box(pdf, 105, y + gap, bw, bh, "4a  Guaranteed payments for services", "0.00")

    add_box(pdf, 10, y + gap*2, bw, bh, "4b  Guaranteed payments for capital", "0.00")
    add_box(pdf, 105, y + gap*2, bw, bh, "4c  Total guaranteed payments", "0.00")

    add_box(pdf, 10, y + gap*3, bw, bh, "5  Interest income", "345.00")
    add_box(pdf, 105, y + gap*3, bw, bh, "6a  Ordinary dividends", "0.00")

    add_box(pdf, 10, y + gap*4, bw, bh, "7  Royalties", "0.00")
    add_box(pdf, 105, y + gap*4, bw, bh, "8  Net short-term capital gain (loss)", "0.00")

    add_box(pdf, 10, y + gap*5, bw, bh, "9a  Net long-term capital gain (loss)", "2,150.00")
    add_box(pdf, 105, y + gap*5, bw, bh, "10  Net section 1231 gain (loss)", "0.00")

    add_box(pdf, 10, y + gap*6, bw, bh, "11  Other income (loss)  See STMT 1", "4,200.00")
    add_box(pdf, 105, y + gap*6, bw, bh, "13  Credits  See STMT 2", "1,850.00")

    # Page 2 - Continuation / Statements
    pdf.add_page()
    pdf.set_font("Helvetica", "B", 12)
    pdf.cell(0, 8, "Schedule K-1 (Form 1065) - Continuation", ln=True, align="C")
    pdf.set_font("Helvetica", "", 8)
    pdf.cell(0, 5, "COOSA VALLEY REAL ESTATE PARTNERS LP    EIN: 58-9876543", ln=True, align="C")
    pdf.cell(0, 5, "Partner: JEFFREY A WATTS    TIN: ***-**-4521", ln=True, align="C")
    pdf.ln(5)

    # STMT 1
    pdf.set_font("Helvetica", "B", 10)
    pdf.cell(0, 7, "STMT 1 - Line 11 - Other Income (Loss) Detail", ln=True)
    pdf.ln(2)

    pdf.set_font("Helvetica", "B", 8)
    pdf.set_fill_color(230, 230, 230)
    pdf.cell(100, 6, "Description", border=1, fill=True)
    pdf.cell(40, 6, "Amount", border=1, fill=True, align="R", ln=True)

    pdf.set_font("Courier", "", 8)
    stmt1_items = [
        ("Section 1245 gain from disposition of assets", "2,800.00"),
        ("Cancellation of debt income", "900.00"),
        ("Other portfolio income", "500.00"),
    ]
    for desc, amt in stmt1_items:
        pdf.cell(100, 5, desc, border=1)
        pdf.cell(40, 5, amt, border=1, align="R", ln=True)
    pdf.set_font("Helvetica", "B", 8)
    pdf.cell(100, 5, "Total Line 11", border=1)
    pdf.cell(40, 5, "4,200.00", border=1, align="R", ln=True)
    pdf.ln(8)

    # STMT 2
    pdf.set_font("Helvetica", "B", 10)
    pdf.cell(0, 7, "STMT 2 - Line 13 - Credits Detail", ln=True)
    pdf.ln(2)

    pdf.set_font("Helvetica", "B", 8)
    pdf.cell(100, 6, "Description", border=1, fill=True)
    pdf.cell(40, 6, "Amount", border=1, fill=True, align="R", ln=True)

    pdf.set_font("Courier", "", 8)
    stmt2_items = [
        ("Low-income housing credit (Section 42(j)(5))", "1,200.00"),
        ("Rehabilitation credit", "650.00"),
    ]
    for desc, amt in stmt2_items:
        pdf.cell(100, 5, desc, border=1)
        pdf.cell(40, 5, amt, border=1, align="R", ln=True)
    pdf.set_font("Helvetica", "B", 8)
    pdf.cell(100, 5, "Total Line 13", border=1)
    pdf.cell(40, 5, "1,850.00", border=1, align="R", ln=True)
    pdf.ln(8)

    # Additional info
    pdf.set_font("Helvetica", "B", 10)
    pdf.cell(0, 7, "Supplemental Information", ln=True)
    pdf.ln(2)
    pdf.set_font("Helvetica", "", 8)
    pdf.cell(0, 5, "Line 14 - Self-employment earnings (loss):  $0.00", ln=True)
    pdf.cell(0, 5, "Line 15a - Net earnings (loss) from self-employment:  $0.00", ln=True)
    pdf.cell(0, 5, "Line 16a - Tax-exempt interest income:  $225.00", ln=True)
    pdf.cell(0, 5, "Line 16c - Nondeductible expenses:  $150.00", ln=True)
    pdf.cell(0, 5, "Line 19 - Distributions:  Cash $6,000.00", ln=True)
    pdf.cell(0, 5, "Line 20 - Other information:  Code AH - Gross receipts for Section 448(c):  $2,450,000", ln=True)

    path = OUT_DIR / "K1_Coosa_Valley_RE_2024.pdf"
    pdf.output(str(path))
    print(f"  Created: {path.name}")
    return path


# ─── Mixed Document (multi-type scan) ───────────────────────────────────────

def create_mixed_scan():
    """Simulates a single scan of multiple different documents — common in practice."""
    pdf = FPDF()

    # Page 1: 1099-R
    pdf.add_page()
    pdf.set_font("Helvetica", "B", 14)
    pdf.cell(0, 8, "Form 1099-R   Distributions From Pensions, Annuities, etc.   2024", ln=True, align="C")
    pdf.set_font("Helvetica", "", 8)
    pdf.cell(0, 5, "Copy B - Report this income on your federal tax return", ln=True, align="C")
    pdf.ln(5)

    pdf.set_font("Helvetica", "", 8)
    pdf.cell(0, 4, "PAYER'S name", ln=True)
    pdf.set_font("Courier", "", 9)
    pdf.cell(0, 5, "GEORGIA TEACHERS RETIREMENT SYSTEM", ln=True)
    pdf.cell(0, 5, "TWO NORTHSIDE 75, SUITE 400", ln=True)
    pdf.cell(0, 5, "ATLANTA, GA 30318", ln=True)
    pdf.ln(2)
    pdf.set_font("Helvetica", "", 8)
    pdf.cell(0, 4, "PAYER'S TIN: 58-6001914    RECIPIENT'S TIN: ***-**-4521", ln=True)
    pdf.cell(0, 4, "RECIPIENT: JEFFREY A WATTS", ln=True)
    pdf.ln(3)

    y = 70
    bw = 85
    bh = 18
    gap = 20
    add_box(pdf, 10, y, bw, bh, "1  Gross distribution", "24,000.00")
    add_box(pdf, 105, y, bw, bh, "2a  Taxable amount", "24,000.00")
    add_box(pdf, 10, y + gap, bw, bh, "2b  Taxable amount not determined", "")
    add_box(pdf, 105, y + gap, bw, bh, "4  Federal income tax withheld", "3,600.00")
    add_box(pdf, 10, y + gap*2, bw, bh, "5  Employee contributions", "0.00")
    add_box(pdf, 105, y + gap*2, bw, bh, "7  Distribution code(s)", "7")
    add_box(pdf, 10, y + gap*3, bw, bh, "9b  Total employee contributions", "0.00")
    add_box(pdf, 105, y + gap*3, bw, bh, "14  State tax withheld", "1,320.00")
    add_box(pdf, 10, y + gap*4, bw, bh, "15  State/Payer's state no.", "GA  58-6001914")
    add_box(pdf, 105, y + gap*4, bw, bh, "16  State distribution", "24,000.00")

    # Page 2: 1099-SSA
    pdf.add_page()
    pdf.set_font("Helvetica", "B", 14)
    pdf.cell(0, 8, "SSA-1099   Social Security Benefit Statement   2024", ln=True, align="C")
    pdf.set_font("Helvetica", "", 8)
    pdf.cell(0, 5, "Department of the Treasury - Internal Revenue Service", ln=True, align="C")
    pdf.ln(5)

    pdf.set_font("Helvetica", "", 8)
    pdf.cell(0, 4, "Name", ln=True)
    pdf.set_font("Courier", "B", 10)
    pdf.cell(0, 5, "JEFFREY A WATTS", ln=True)
    pdf.set_font("Helvetica", "", 8)
    pdf.cell(0, 4, "Beneficiary's Social Security Number: ***-**-4521", ln=True)
    pdf.ln(5)

    y = 55
    add_box(pdf, 10, y, 180, 20, "3  Total benefits paid in 2024", "22,560.00", value_size=12)
    add_box(pdf, 10, y + 25, 85, 18, "4  Benefits repaid to SSA in 2024", "0.00")
    add_box(pdf, 105, y + 25, 85, 18, "5  Net benefits for 2024 (Box 3 minus Box 4)", "22,560.00")
    add_box(pdf, 10, y + 50, 85, 18, "6  Voluntary federal tax withheld", "2,820.00")

    pdf.set_font("Helvetica", "", 8)
    pdf.set_xy(10, y + 75)
    pdf.cell(0, 4, "Description of Amount in Box 3:", ln=True)
    pdf.set_font("Courier", "", 8)
    pdf.cell(0, 4, "  Paid by check or direct deposit:  Jan $1,880.00 x 12 = $22,560.00", ln=True)

    path = OUT_DIR / "Mixed_1099R_SSA1099_2024.pdf"
    pdf.output(str(path))
    print(f"  Created: {path.name}")
    return path


# ─── Main ────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print(f"\nGenerating test PDFs in: {OUT_DIR}\n")

    create_w2()
    create_1099_int()
    create_1099_div()
    create_k1()
    create_bank_statement()
    create_mixed_scan()

    print(f"\nDone! {len(list(OUT_DIR.glob('*.pdf')))} PDFs ready in {OUT_DIR}")
    print("Upload these through the dashboard at http://localhost:5050")

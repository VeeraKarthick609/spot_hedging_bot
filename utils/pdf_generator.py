from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.lib import colors
from reportlab.lib.units import inch
from datetime import datetime
import json

def create_report_pdf(filename: str, report_data: dict):
    """Generates a formal PDF report."""
    doc = SimpleDocTemplate(filename, rightMargin=inch/2, leftMargin=inch/2, topMargin=inch/2, bottomMargin=inch/2)
    styles = getSampleStyleSheet()
    story = []

    # Title
    story.append(Paragraph("Portfolio Risk & Compliance Report", styles['h1']))
    story.append(Paragraph(f"Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}", styles['Normal']))
    story.append(Spacer(1, 0.2*inch))
    
    # Position Snapshot
    story.append(Paragraph("1. Position Snapshot", styles['h2']))
    pos_data = [['Asset', 'Type', 'Quantity', 'Market Price', 'Value (USD)']]
    for pos in report_data['positions']:
        pos_data.append([
            pos['asset'], pos['type'], f"{pos['size']:.4f}", f"${pos['price']:,.2f}", f"${pos['value']:,.2f}"
        ])
    pos_table = Table(pos_data)
    pos_table.setStyle(TableStyle([
        ('BACKGROUND', (0,0), (-1,0), colors.grey),
        ('TEXTCOLOR', (0,0), (-1,0), colors.whitesmoke),
        ('ALIGN', (0,0), (-1,-1), 'CENTER'),
        ('FONTNAME', (0,0), (-1,0), 'Helvetica-Bold'),
        ('BOTTOMPADDING', (0,0), (-1,0), 12),
        ('BACKGROUND', (0,1), (-1,-1), colors.beige),
        ('GRID', (0,0), (-1,-1), 1, colors.black)
    ]))
    story.append(pos_table)
    story.append(Spacer(1, 0.2*inch))

    # Risk Metrics
    story.append(Paragraph("2. Key Risk Metrics", styles['h2']))
    risk = report_data['risk_metrics']
    risk_data = [
        ['Metric', 'Value'],
        ['Total Delta Exposure', f"${risk['delta']:,.2f}"],
        ['1-Day 95% VaR', f"${risk['var']:,.2f}"]
    ]
    risk_table = Table(risk_data, colWidths=[3*inch, 2*inch])
    risk_table.setStyle(TableStyle([
        ('BACKGROUND', (0,0), (-1,0), colors.grey),
        ('TEXTCOLOR',(0,0),(-1,0),colors.whitesmoke),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('GRID', (0,0), (-1,-1), 1, colors.black)
    ]))
    story.append(risk_table)
    story.append(Spacer(1, 0.2*inch))
    
    # Audit Trail
    story.append(Paragraph("3. Audit Trail (Recent Hedges)", styles['h2']))
    history_data = [['Timestamp', 'Action', 'Size', 'Venue', 'Cost (USD)']]
    for item in report_data['history']:
        details = json.loads(item['details'])
        history_data.append([
            item['timestamp'],
            item['action'].upper(),
            f"{abs(item['size']):.4f}",
            details.get('venue', 'N/A').upper(),
            f"${details.get('total_cost_usd', 0):,.2f}"
        ])
    history_table = Table(history_data)
    history_table.setStyle(TableStyle([
        ('BACKGROUND', (0,0), (-1,0), colors.grey),
        ('TEXTCOLOR', (0,0), (-1,0), colors.whitesmoke),
        ('FONTNAME', (0,0), (-1,0), 'Helvetica-Bold'),
        ('GRID', (0,0), (-1,-1), 1, colors.black)
    ]))
    story.append(history_table)
    
    doc.build(story)
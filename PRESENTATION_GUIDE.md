# Presentation Guide

## How to View the Presentation

### Option 1: Open in Browser (Recommended)

Simply open `presentation.html` in any modern web browser:

```bash
# On Linux
xdg-open presentation.html

# On macOS
open presentation.html

# On Windows
start presentation.html

# Or manually: Right-click file > Open with > Chrome/Firefox
```

The presentation will load directly from CDN resources (no local server needed).

### Option 2: Python HTTP Server

If you prefer to serve it locally:

```bash
python3 -m http.server 8000
# Then open: http://localhost:8000/presentation.html
```

## Navigation

- **Arrow Keys** or **Space**: Next slide
- **Shift + Space**: Previous slide
- **Home**: First slide
- **End**: Last slide
- **Esc** or **O**: Overview mode (see all slides)
- **F**: Fullscreen mode
- **S**: Speaker notes (if available)
- **?**: Help menu

## Presentation Structure

1. **Title & Agenda** (2 slides)
2. **Project Overview** (2 slides) - Use case and business value
3. **Data Sources** (2 slides) - Marketplace datasets
4. **Data Model** (4 slides) - Architecture, staging, intermediate, marts
5. **Data Governance** (4 slides) - Testing, materialization, documentation
6. **Dashboard** (2 slides) - Streamlit features and code
7. **Key Findings** (3 slides) - Metrics and insights
8. **Production Architecture** (5 slides) - Scaling, workflows, monitoring
9. **Challenges** (5 slides) - Technical problems solved
10. **Future Improvements** (3 slides) - Enhancement ideas
11. **Tech Stack & Summary** (2 slides)
12. **Q&A** (1 slide)

**Total: ~35 slides | Estimated time: 20-25 minutes**

## Tips for Presentation Day

### Before the Presentation

1. **Test the presentation file** - Ensure all slides load properly
2. **Prepare demo data** - Have Streamlit dashboard ready to show
3. **Review key metrics** - 240 high-opportunity blocks, 436K people
4. **Rehearse timing** - Aim for 23-24 minutes to leave time for buffer

### During Presentation

- **Start strong**: Emphasize the 240 underserved census blocks finding
- **Show live demo**: Launch Streamlit dashboard during Dashboard section
- **Highlight testing**: 38 tests, 100% pass rate shows data quality focus
- **Architecture emphasis**: Production readiness and scalability
- **Be honest about challenges**: Shows problem-solving skills

### Suggested Demo Flow

When you reach the Dashboard slides:

```bash
# Have this terminal ready
uv run streamlit run streamlit_app.py
# Opens at http://localhost:8501
```

**Demo points:**
1. Show choropleth map colored by opportunity score
2. Filter to "High Opportunity" only
3. Click on a census block to show popup details
4. Toggle gym locations overlay
5. Show data export functionality

### Questions to Prepare For

**Technical:**
- How would you handle real-time data updates?
- What about PII/GDPR compliance?
- How do you ensure data quality at scale?
- What's your testing strategy?

**Business:**
- How accurate is the demand score?
- What other factors should be considered?
- How would this scale to other cities?

**Process:**
- How long did this take?
- What was the hardest part?
- What would you do differently?

## Compensation Discussion

**Preparation (as requested in case requirements):**

Research typical Data Engineer salaries in your location:
- Check Glassdoor, levels.fyi for LEAP or similar companies
- Consider: experience level, location, company size
- Prepare a range (not a single number)
- Factor in: base salary, bonus, equity (if applicable)
- Benefits: PTO, remote work, learning budget, etc.

**When can you start:**
- Standard notice period: 2-4 weeks
- Consider: project wrap-up, knowledge transfer
- Be realistic but show enthusiasm

## Backup Plans

**If internet fails:**
- Presentation works offline (loads from CDN cache)
- Have screenshots of dashboard in case Snowflake is unreachable

**If time runs short:**
- Skip "Future Improvements" (less critical)
- Condense Challenges section to top 2 problems

**If technical issues:**
- Have PDF backup of presentation
- Have architecture diagram saved as image

## Post-Presentation

Files to submit (as per case requirements):

1. ✅ GitHub repository (entire project)
2. ✅ `presentation.html` (this file)
3. ✅ `PRODUCTION_ARCHITECTURE.md`
4. ✅ `README.md`

**Submit these the day before your presentation!**

---

Good luck! You've built something comprehensive and production-ready. Be confident! 🎯

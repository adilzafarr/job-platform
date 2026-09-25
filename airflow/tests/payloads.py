"""Provider payload builders modelled on responses observed on 2026-09-25."""

from __future__ import annotations

import json
from typing import Any

HIMALAYAS_URL = "https://himalayas.app/jobs/api"
REMOTEOK_URL = "https://remoteok.com/api"
REMOTIVE_URL = "https://remotive.com/api/remote-jobs"
WWR_BASE = "https://weworkremotely.com/categories"


def himalayas_job(n: int, pub: int) -> dict[str, Any]:
    return {
        "title": f"Engineer {n}",
        "excerpt": "Build things",
        "companyName": "Acme",
        "companySlug": "acme",
        "companyLogo": "https://cdn.example/logo.png",
        "employmentType": "Full Time",
        "minSalary": 100000 if n % 2 else None,
        "maxSalary": 150000 if n % 2 else None,
        "salaryPeriod": "annual",
        "seniority": ["Senior"],
        "currency": "USD",
        "locationRestrictions": [],
        "timezoneRestrictions": [],
        "categories": ["Software-Engineer"],
        "parentCategories": ["Engineering"],
        "description": "<p>Job</p>",
        "pubDate": pub,
        "expiryDate": pub + 60 * 86400,
        "applicationLink": f"https://himalayas.app/apply/{n}",
        "guid": f"https://himalayas.app/companies/acme/jobs/engineer-{n}",
    }


def himalayas_page(jobs: list[dict], next_cursor: str | None, total: int = 98768) -> str:
    return json.dumps(
        {
            "comments": "cursor pagination",
            "updatedAt": 1790299354,
            "offset": 0,
            "limit": 20,
            "totalCount": total,
            "nextCursor": next_cursor,
            "jobs": jobs,
        }
    )


LEGAL = {"last_updated": 1790265606, "legal": "API Terms of Service: Please link back ..."}


def remoteok_job(n: int, epoch: int = 1790208007) -> dict[str, Any]:
    return {
        "slug": f"remote-dev-{n}",
        "id": str(1137400 + n),
        "epoch": epoch - n,
        "date": "2026-09-24T00:00:07+00:00",
        "company": "Acme",
        "company_logo": "",
        "position": f"Developer {n}",
        "tags": ["python", "dev"],
        "description": "<p>desc</p>",
        "location": "Worldwide",
        "apply_url": f"https://remoteok.com/l/{n}",
        "salary_min": 0,
        "salary_max": 0,
        "logo": "",
        "url": f"https://remoteok.com/remote-jobs/{n}",
    }


def remoteok_body(jobs: list[dict], legal: bool = True) -> str:
    return json.dumps(([LEGAL] if legal else []) + jobs)


def remotive_job(n: int) -> dict[str, Any]:
    return {
        "id": 2090000 + n,
        "url": f"https://remotive.com/remote-jobs/software-dev/job-{n}",
        "title": f"Backend Engineer {n}",
        "company_name": "Acme",
        "company_logo": "https://remotive.com/logo.png",
        "category": "Software Development",
        "tags": ["python"],
        "job_type": "full_time",
        "publication_date": "2026-09-21T12:55:11",
        "candidate_required_location": "Worldwide",
        "salary": "$100k",
        "description": "<p>desc</p>",
    }


def remotive_body(jobs: list[dict]) -> str:
    return json.dumps(
        {
            "00-warning": "Remotive main domain moved",
            "0-legal-notice": "Legal warning",
            "job-count": len(jobs),
            "total-job-count": len(jobs),
            "jobs": jobs,
        }
    )


def wwr_item(slug: str, *, media: bool = False) -> str:
    media_el = '<media:content url="https://wwr.example/logo.png" medium="image"/>' if media else ""
    return f"""
    <item>
      <title>Acme: {slug}</title>
      <region>Anywhere in the World</region>
      <country>🇺🇸 United States</country>
      <state></state>
      <skills>Python, SQL</skills>
      <category>Design</category>
      <type>Full-Time</type>
      <description>&lt;p&gt;Job {slug}&lt;/p&gt;</description>
      <pubDate>Fri, 25 Sep 2026 00:33:21 +0000</pubDate>
      <expires_at>Sun, 25 Oct 2026 00:33:21 +0000</expires_at>
      <guid>https://weworkremotely.com/remote-jobs/{slug}</guid>
      <link>https://weworkremotely.com/remote-jobs/{slug}</link>
      {media_el}
    </item>"""


def wwr_feed(items: list[str]) -> str:
    return f"""<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0" xmlns:dc="http://purl.org/dc/elements/1.1/" xmlns:media="http://search.yahoo.com/mrss">
  <channel>
    <title>We Work Remotely: Design Jobs</title>
    <link>https://weworkremotely.com/categories/remote-design-jobs.rss</link>
    <description>jobs</description>
    <language>en-US</language>
    <ttl>60</ttl>
    {''.join(items)}
  </channel>
</rss>"""

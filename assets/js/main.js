// === SEARCH ===
const searchData = [];

// Build search index from all post titles (injected by Jekyll via JSON endpoint)
async function buildSearchIndex() {
  try {
    const baseUrl = document.querySelector('base')?.href || window.location.origin;
    const res = await fetch('/amazon-associates-content-site/search.json');
    if (res.ok) {
      const posts = await res.json();
      posts.forEach(p => searchData.push(p));
    }
  } catch (e) {}
}

function runSearch(query) {
  const results = document.getElementById('search-results');
  if (!query || query.length < 2) { results.innerHTML = ''; return; }

  const q = query.toLowerCase();
  const matches = searchData.filter(p =>
    p.title.toLowerCase().includes(q) ||
    (p.description && p.description.toLowerCase().includes(q)) ||
    (p.categories && p.categories.join(' ').toLowerCase().includes(q))
  ).slice(0, 8);

  if (matches.length === 0) {
    results.innerHTML = '<p style="padding:12px;color:#888;font-size:13px;">No results found.</p>';
    return;
  }

  results.innerHTML = matches.map(p => `
    <a href="${p.url}" class="search-result-item">
      <span class="search-result-category">${(p.categories?.[0] || '').replace(/-/g,' ')}</span>
      <span class="search-result-title">${p.title}</span>
    </a>
  `).join('');
}

// Close search on outside click
document.addEventListener('click', (e) => {
  const overlay = document.getElementById('search-overlay');
  if (overlay?.classList.contains('active') && e.target === overlay) {
    overlay.classList.remove('active');
  }
});

// === TABLE OF CONTENTS ===
function buildTOC() {
  const toc = document.getElementById('toc');
  if (!toc) return;

  const headings = document.querySelectorAll('.post-content h2, .post-content h3');
  if (headings.length < 2) {
    toc.closest('.toc-widget')?.remove();
    return;
  }

  headings.forEach((h, i) => {
    if (!h.id) h.id = 'section-' + i;
    const a = document.createElement('a');
    a.href = '#' + h.id;
    a.textContent = h.textContent;
    if (h.tagName === 'H3') a.style.paddingLeft = '16px';
    toc.appendChild(a);
  });
}

// === HIGHLIGHT ACTIVE TOC ITEM ON SCROLL ===
function initScrollSpy() {
  const tocLinks = document.querySelectorAll('.toc-links a');
  if (!tocLinks.length) return;

  const observer = new IntersectionObserver((entries) => {
    entries.forEach(entry => {
      if (entry.isIntersecting) {
        tocLinks.forEach(l => l.style.background = '');
        const active = document.querySelector(`.toc-links a[href="#${entry.target.id}"]`);
        if (active) active.style.background = 'var(--cream-mid)';
      }
    });
  }, { rootMargin: '-20% 0px -70% 0px' });

  document.querySelectorAll('.post-content h2, .post-content h3').forEach(h => observer.observe(h));
}

// === INIT ===
document.addEventListener('DOMContentLoaded', () => {
  buildTOC();
  initScrollSpy();
  buildSearchIndex();

  // Keyboard shortcut: "/" to open search
  document.addEventListener('keydown', (e) => {
    if (e.key === '/' && !e.target.matches('input, textarea')) {
      e.preventDefault();
      document.getElementById('search-overlay')?.classList.add('active');
      setTimeout(() => document.getElementById('search-input')?.focus(), 50);
    }
    if (e.key === 'Escape') {
      document.getElementById('search-overlay')?.classList.remove('active');
    }
  });
});

class NewsAggregator {
	constructor() {
		this.currentOffset = 0;
		this.currentLimit = 20;
		this.currentFilters = {
			category: 'all',
			search: '',
			source: '',
		};

		this.init();
	}

	async init() {
		await this.loadStats();
		await this.loadSources();
		await this.loadArticles();
		this.bindEvents();
	}

	async loadStats() {
		try {
			const response = await fetch('/api/stats');
			const stats = await response.json();

			// Update header stats
			document.getElementById('totalArticles').textContent =
				stats.total_articles.toLocaleString();
			document.getElementById('recentArticles').textContent =
				stats.recent_articles.toLocaleString();
			document.getElementById('totalSources').textContent = Object.keys(
				stats.top_sources
			).length;

			// Update dashboard
			this.updateTopTopics(stats.topics);
			this.updateTopSources(stats.top_sources);
		} catch (error) {
			console.error('Error loading stats:', error);
		}
	}

	updateTopTopics(topics) {
		const container = document.getElementById('topTopics');
		const topicColors = {
			technology: '#319795',
			business: '#d69e2e',
			ai: '#b83280',
			stocks: '#38a169',
			startups: '#3182ce',
			politics: '#e53e3e',
			general: '#718096',
		};

		const html = Object.entries(topics)
			.slice(0, 6)
			.map(
				([topic, count]) => `
                <div class="topic-item">
                    <span class="topic-badge" style="background-color: ${
											topicColors[topic] || '#718096'
										}20; color: ${topicColors[topic] || '#718096'}">
                        ${topic.charAt(0).toUpperCase() + topic.slice(1)}
                    </span>
                    <span>${count} articles</span>
                </div>
            `
			)
			.join('');

		container.innerHTML = html;
	}

	updateTopSources(sources) {
		const container = document.getElementById('topSources');
		const html = Object.entries(sources)
			.slice(0, 6)
			.map(
				([source, count]) => `
                <div class="source-item">
                    <span>${source}</span>
                    <span>${count} articles</span>
                </div>
            `
			)
			.join('');

		container.innerHTML = html;
	}

	async loadSources() {
		try {
			const response = await fetch('/api/sources');
			const data = await response.json();
			const sourceSelect = document.getElementById('sourceFilter');

			// Clear existing options (except "All Sources")
			sourceSelect.innerHTML = '<option value="">All Sources</option>';

			// Add source options
			data.sources.forEach((source) => {
				const option = document.createElement('option');
				option.value = source.name;
				option.textContent = `${source.name} (${
					source.total_articles_collected || 0
				})`;
				sourceSelect.appendChild(option);
			});
		} catch (error) {
			console.error('Error loading sources:', error);
		}
	}

	async loadArticles(reset = false) {
		if (reset) {
			this.currentOffset = 0;
			document.getElementById('articlesGrid').innerHTML = '';
		}

		const loadingIndicator = document.getElementById('loadingIndicator');
		loadingIndicator.style.display = 'block';

		try {
			const params = new URLSearchParams({
				limit: this.currentLimit,
				offset: this.currentOffset,
				...this.currentFilters,
			});

			// Remove empty values
			Object.keys(this.currentFilters).forEach((key) => {
				if (!this.currentFilters[key] || this.currentFilters[key] === 'all') {
					params.delete(key);
				}
			});

			const response = await fetch(`/api/articles?${params}`);
			const data = await response.json();

			this.renderArticles(data.articles, reset);

			// Show/hide load more button
			const loadMoreBtn = document.getElementById('loadMoreBtn');
			if (data.articles.length === this.currentLimit) {
				loadMoreBtn.style.display = 'block';
			} else {
				loadMoreBtn.style.display = 'none';
			}
		} catch (error) {
			console.error('Error loading articles:', error);
			document.getElementById('articlesGrid').innerHTML =
				'<div class="error">Error loading articles. Please try again.</div>';
		} finally {
			loadingIndicator.style.display = 'none';
		}
	}

	renderArticles(articles, reset = false) {
		const grid = document.getElementById('articlesGrid');

		if (reset) {
			grid.innerHTML = '';
		}

		if (articles.length === 0 && reset) {
			grid.innerHTML = `
                <div class="no-results">
                    <h3>No articles found</h3>
                    <p>Try adjusting your search filters</p>
                </div>
            `;
			return;
		}

		articles.forEach((article) => {
			const articleElement = this.createArticleCard(article);
			grid.appendChild(articleElement);
		});

		this.currentOffset += articles.length;
	}

	createArticleCard(article) {
		const card = document.createElement('div');
		card.className = 'article-card';

		const publishedDate = article.published_at
			? new Date(article.published_at).toLocaleDateString()
			: 'Unknown date';

		const discoveredDate = article.discovered_at
			? new Date(article.discovered_at).toLocaleTimeString()
			: '';

		const qualityClass =
			article.quality_score >= 75
				? 'score-good'
				: article.quality_score >= 50
				? 'score-average'
				: 'score-poor';

		card.innerHTML = `
            <div class="article-content">
                <div class="article-header">
                    <span class="article-topic topic-${
											article.primary_topic || 'general'
										}">
                        ${(article.primary_topic || 'general').toUpperCase()}
                    </span>
                    <span class="quality-score ${qualityClass}">
                        ⭐ ${Math.round(article.quality_score || 0)}
                    </span>
                </div>
                
                <h3 class="article-title">${article.title}</h3>
                
                <div class="article-summary">
                    ${
											article.summary ||
											article.content?.substring(0, 200) + '...' ||
											'No content available'
										}
                </div>
                
                <div class="article-meta">
                    <div class="article-source">${article.source_name}</div>
                    <div class="article-stats">
                        <span>📅 ${publishedDate}</span>
                        <span>📖 ${
													article.reading_time_minutes || 1
												}m read</span>
                        <span>📊 ${article.word_count || 0} words</span>
                    </div>
                </div>
            </div>
        `;

		card.addEventListener('click', () => this.showArticleModal(article));
		return card;
	}

	showArticleModal(article) {
		const modal = document.getElementById('articleModal');
		const modalContent = document.getElementById('modalContent');

		const publishedDate = article.published_at
			? new Date(article.published_at).toLocaleString()
			: 'Unknown date';

		modalContent.innerHTML = `
            <div style="padding: 2rem;">
                <div style="margin-bottom: 1rem;">
                    <span class="article-topic topic-${
											article.primary_topic || 'general'
										}" style="margin-right: 1rem;">
                        ${(article.primary_topic || 'general').toUpperCase()}
                    </span>
                    <span class="quality-score">Quality Score: ${Math.round(
											article.quality_score || 0
										)}/100</span>
                </div>
                
                <h1 style="margin-bottom: 1rem; color: #2d3748;">${
									article.title
								}</h1>
                
                <div style="margin-bottom: 1.5rem; color: #718096; font-size: 0.875rem;">
                    <strong>Source:</strong> ${article.source_name} | 
                    <strong>Published:</strong> ${publishedDate} | 
                    <strong>Region:</strong> ${article.primary_region} |
                    <strong>Reading Time:</strong> ${
											article.reading_time_minutes || 1
										} minutes
                </div>
                
                ${
									article.summary
										? `
                    <div style="background: #f7fafc; padding: 1rem; border-radius: 8px; margin-bottom: 1.5rem;">
                        <strong>Summary:</strong> ${article.summary}
                    </div>
                `
										: ''
								}
                
                <div style="line-height: 1.7; color: #4a5568;">
                    ${
											article.content
												? article.content.replace(/\n/g, '<br>')
												: 'No content available'
										}
                </div>
                
                ${
									article.countries_mentioned &&
									article.countries_mentioned.length > 0
										? `
                    <div style="margin-top: 1.5rem; padding-top: 1rem; border-top: 1px solid #e2e8f0;">
                        <strong>Countries Mentioned:</strong> ${article.countries_mentioned.join(
													', '
												)}
                    </div>
                `
										: ''
								}
                
                <div style="margin-top: 1.5rem; text-align: center;">
                    <a href="${
											article.url
										}" target="_blank" style="background: #667eea; color: white; padding: 0.75rem 1.5rem; border-radius: 8px; text-decoration: none; display: inline-block;">
                        Read Original Article →
                    </a>
                </div>
            </div>
        `;

		modal.style.display = 'flex';
	}

	bindEvents() {
		// Search functionality
		const searchBtn = document.getElementById('searchBtn');
		const searchInput = document.getElementById('searchInput');

		searchBtn.addEventListener('click', () => this.handleSearch());
		searchInput.addEventListener('keypress', (e) => {
			if (e.key === 'Enter') this.handleSearch();
		});

		// Filter functionality
		document
			.getElementById('categoryFilter')
			.addEventListener('change', (e) => {
				this.currentFilters.category = e.target.value;
				this.loadArticles(true);
			});

		document.getElementById('sourceFilter').addEventListener('change', (e) => {
			this.currentFilters.source = e.target.value;
			this.loadArticles(true);
		});

		// Refresh button
		document.getElementById('refreshBtn').addEventListener('click', () => {
			this.loadStats();
			this.loadArticles(true);
		});

		// Load more button
		document.getElementById('loadMoreBtn').addEventListener('click', () => {
			this.loadArticles(false);
		});

		// Modal close
		document.getElementById('closeModal').addEventListener('click', () => {
			document.getElementById('articleModal').style.display = 'none';
		});

		// Click outside modal to close
		document.getElementById('articleModal').addEventListener('click', (e) => {
			if (e.target.id === 'articleModal') {
				document.getElementById('articleModal').style.display = 'none';
			}
		});
	}

	handleSearch() {
		const searchInput = document.getElementById('searchInput');
		this.currentFilters.search = searchInput.value.trim();
		this.loadArticles(true);
	}
}

// Initialize the app when DOM is loaded
document.addEventListener('DOMContentLoaded', () => {
	new NewsAggregator();
});

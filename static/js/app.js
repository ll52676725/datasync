let currentTab = 'dashboard';
let currentTaskId = null;
let progressPollingInterval = null;
let configNameForEdit = null;
let selectedTables = [];

const pageTitles = {
    dashboard: { title: '仪表盘', subtitle: '查看同步系统概览' },
    configs: { title: '配置管理', subtitle: '管理数据库同步配置' },
    tasks: { title: '任务监控', subtitle: '监控和管理同步任务' },
    progress: { title: '断点续传', subtitle: '管理同步断点和进度' }
};

document.addEventListener('DOMContentLoaded', function() {
    initNavigation();
    initConfigTabs();
    loadDashboard();
    startDashboardPolling();
});

function initNavigation() {
    document.querySelectorAll('.nav-item').forEach(item => {
        item.addEventListener('click', function(e) {
            e.preventDefault();
            const tab = this.dataset.tab;
            switchTab(tab);
        });
    });
}

function switchTab(tab) {
    document.querySelectorAll('.nav-item').forEach(item => {
        item.classList.toggle('active', item.dataset.tab === tab);
    });

    document.querySelectorAll('.tab-content').forEach(content => {
        content.classList.remove('active');
    });
    document.getElementById(`tab-${tab}`).classList.add('active');

    const title = pageTitles[tab];
    if (title) {
        document.getElementById('pageTitle').textContent = title.title;
        document.getElementById('pageSubtitle').textContent = title.subtitle;
    }

    currentTab = tab;

    if (tab === 'configs') loadConfigs();
    if (tab === 'tasks') loadTasks();
    if (tab === 'progress') loadProgress();
    if (tab === 'dashboard') loadDashboard();
}

function initConfigTabs() {
    document.querySelectorAll('.config-tab').forEach(tab => {
        tab.addEventListener('click', function() {
            const section = this.dataset.section;
            
            document.querySelectorAll('.config-tab').forEach(t => t.classList.remove('active'));
            this.classList.add('active');
            
            document.querySelectorAll('.config-section').forEach(s => s.classList.remove('active'));
            document.getElementById(`section-${section}`).classList.add('active');
        });
    });
}

function showToast(message, type = 'info') {
    const toast = document.getElementById('toast');
    toast.className = `toast ${type}`;
    toast.textContent = message;
    toast.classList.add('show');
    
    setTimeout(() => {
        toast.classList.remove('show');
    }, 3000);
}

async function apiRequest(url, method = 'GET', data = null) {
    const options = {
        method,
        headers: {
            'Content-Type': 'application/json'
        }
    };
    
    if (data) {
        options.body = JSON.stringify(data);
    }
    
    try {
        const response = await fetch(url, options);
        
        if (response.status === 401) {
            window.location.href = '/login';
            return null;
        }
        
        const result = await response.json();
        return result;
    } catch (error) {
        console.error('API Error:', error);
        showToast('网络错误，请稍后重试', 'error');
        return null;
    }
}

function formatNumber(num) {
    if (num >= 10000) {
        return (num / 10000).toFixed(1) + '万';
    }
    return num.toLocaleString();
}

function formatDate(dateStr) {
    if (!dateStr) return '-';
    const date = new Date(dateStr);
    return date.toLocaleString('zh-CN');
}

function getStatusBadge(status) {
    const statusMap = {
        pending: { label: '等待中', class: 'status-pending' },
        running: { label: '运行中', class: 'status-running' },
        completed: { label: '已完成', class: 'status-completed' },
        failed: { label: '失败', class: 'status-failed' },
        stopped: { label: '已停止', class: 'status-stopped' },
        stopping: { label: '停止中', class: 'status-stopping' },
        done: { label: '已完成', class: 'status-completed' }
    };
    const info = statusMap[status] || { label: status, class: 'status-pending' };
    return `<span class="status-badge ${info.class}">${info.label}</span>`;
}

function getModeLabel(mode) {
    const modeMap = {
        resume: '断点续传',
        restart: '重新开始'
    };
    return modeMap[mode] || mode;
}

async function loadDashboard() {
    const data = await apiRequest('/api/dashboard');
    if (!data || !data.success) return;

    const stats = data.stats;
    document.getElementById('statTotalTasks').textContent = stats.total_tasks;
    document.getElementById('statCompleted').textContent = stats.completed_tasks;
    document.getElementById('statRunning').textContent = stats.running_tasks;
    document.getElementById('statFailed').textContent = stats.failed_tasks;

    const percentage = stats.total_rows > 0 ? Math.round((stats.synced_rows / stats.total_rows) * 100) : 0;
    document.getElementById('overallPercentage').textContent = percentage + '%';
    updateProgressRing(percentage);
    
    document.getElementById('statCompletedTables').textContent = stats.completed_tables;
    document.getElementById('statTotalTables').textContent = stats.total_tables_in_progress;
    document.getElementById('statSyncedRows').textContent = formatNumber(stats.synced_rows);
    document.getElementById('statTotalRows').textContent = formatNumber(stats.total_rows);

    renderRecentTasks(data.recent_tasks);
}

function updateProgressRing(percentage) {
    const circle = document.getElementById('overallProgressCircle');
    const circumference = 2 * Math.PI * 50;
    const offset = circumference - (percentage / 100) * circumference;
    circle.style.strokeDashoffset = offset;
}

function renderRecentTasks(tasks) {
    const container = document.getElementById('recentTasksList');
    
    if (!tasks || tasks.length === 0) {
        container.innerHTML = `
            <div class="empty-state">
                <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                    <path d="M12 2v4M12 18v4M4.93 4.93l2.83 2.83M16.24 16.24l2.83 2.83M2 12h4M18 12h4M4.93 19.07l2.83-2.83M16.24 7.76l2.83-2.83"></path>
                </svg>
                <h4>暂无任务</h4>
                <p>创建您的第一个同步任务</p>
            </div>
        `;
        return;
    }

    container.innerHTML = tasks.map(task => `
        <div class="task-item-mini" onclick="viewTaskDetail('${task.task_id}')">
            <div class="task-info">
                <div class="task-name">${task.config_name}</div>
                <div class="task-meta">${getModeLabel(task.mode)} · ${formatDate(task.created_at)}</div>
            </div>
            ${getStatusBadge(task.status)}
        </div>
    `).join('');
}

function startDashboardPolling() {
    setInterval(() => {
        if (currentTab === 'dashboard') {
            loadDashboard();
        }
    }, 5000);
}

async function loadConfigs() {
    const data = await apiRequest('/api/configs');
    if (!data || !data.success) return;

    const container = document.getElementById('configsList');
    
    if (data.configs.length === 0) {
        container.innerHTML = `
            <div class="empty-state">
                <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                    <path d="M12.22 2h-.44a2 2 0 0 0-2 2v.18a2 2 0 0 1-3.06 1.87l-.15-.08a2 2 0 0 0-2.73.73l-.43.43a2 2 0 0 0 0 2.82l.09.15a2 2 0 0 1-.25 2.94l-.18.1a2 2 0 0 0 0 3.54l.18.1a2 2 0 0 1 0 2.94l-.09.15a2 2 0 0 0 0 2.82l.43.43a2 2 0 0 0 2.73.73l.15-.08a2 2 0 0 1 3.06 1.87V20a2 2 0 0 0 2 2h.44a2 2 0 0 0 2-2v-.18a2 2 0 0 1 3.06-1.87l.15.08a2 2 0 0 0 2.73-.73l.43-.43a2 2 0 0 0 0-2.82l-.09-.15a2 2 0 0 1 .25-2.94l.18-.1a2 2 0 0 0 0-3.54l-.18-.1a2 2 0 0 1 0-2.94l.09-.15a2 2 0 0 0 0-2.82l-.43-.43a2 2 0 0 0-2.73-.73l-.15.08A2 2 0 0 1 14.22 4.18V4a2 2 0 0 0-2-2z"></path>
                    <circle cx="12" cy="12" r="3"></circle>
                </svg>
                <h4>暂无配置</h4>
                <p>创建您的第一个同步配置</p>
            </div>
        `;
        return;
    }

    container.innerHTML = data.configs.map(config => `
        <div class="config-card">
            <div class="config-header">
                <div class="config-title">
                    <h4>${config.name}</h4>
                    <span>创建者: ${config.created_by} · ${formatDate(config.created_at)}</span>
                </div>
                <div class="config-actions">
                    <button class="icon-btn" onclick="editConfig('${config.name}')" title="编辑">
                        <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                            <path d="M11 4H4a2 2 0 0 0-2 2v14a2 2 0 0 0 2 2h14a2 2 0 0 0 2-2v-7"></path>
                            <path d="M18.5 2.5a2.121 2.121 0 0 1 3 3L12 15l-4 1 1-4 9.5-9.5z"></path>
                        </svg>
                    </button>
                    <button class="icon-btn" onclick="testConfigConnection('${config.name}')" title="测试连接">
                        <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                            <path d="M22 11.08V12a10 10 0 1 1-5.93-9.14"></path>
                            <polyline points="22 4 12 14.01 9 11.01"></polyline>
                        </svg>
                    </button>
                    <button class="icon-btn" onclick="createTaskFromConfig('${config.name}')" title="创建任务">
                        <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                            <polygon points="5 3 19 12 5 21 5 3"></polygon>
                        </svg>
                    </button>
                    <button class="icon-btn danger" onclick="deleteConfig('${config.name}')" title="删除">
                        <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                            <polyline points="3 6 5 6 21 6"></polyline>
                            <path d="M19 6v14a2 2 0 0 1-2 2H7a2 2 0 0 1-2-2V6m3 0V4a2 2 0 0 1 2-2h4a2 2 0 0 1 2 2v2"></path>
                        </svg>
                    </button>
                </div>
            </div>
            <div class="config-details">
                <div class="detail-item">
                    <span class="label">源数据库</span>
                    <span class="value">${config.config.source.host}:${config.config.source.port}/${config.config.source.database}</span>
                </div>
                <div class="detail-item">
                    <span class="label">目标数据库</span>
                    <span class="value">${config.config.target.host}:${config.config.target.port}/${config.config.target.database}</span>
                </div>
                <div class="detail-item">
                    <span class="label">并行表数</span>
                    <span class="value">${config.config.sync.parallel_tables || 4}</span>
                </div>
                <div class="detail-item">
                    <span class="label">同步表数</span>
                    <span class="value">${(config.config.sync.tables || []).length || '全部'}</span>
                </div>
            </div>
        </div>
    `).join('');
}

function showConfigModal(configName = null) {
    configNameForEdit = configName;
    const modal = document.getElementById('configModal');
    const title = document.getElementById('configModalTitle');
    const form = document.getElementById('configForm');
    
    form.reset();
    document.getElementById('tablesCheckboxes').style.display = 'none';
    document.getElementById('tablesCheckboxes').innerHTML = '';
    selectedTables = [];
    
    if (configName) {
        title.textContent = '编辑配置';
        loadConfigForEdit(configName);
    } else {
        title.textContent = '新建配置';
        document.getElementById('configName').value = '';
        document.getElementById('sourcePort').value = 3306;
        document.getElementById('targetPort').value = 3306;
        document.getElementById('syncParallel').value = 4;
        document.getElementById('syncChunkSize').value = 50000;
        document.getElementById('syncBatchSize').value = 5000;
    }
    
    modal.classList.add('active');
}

function closeConfigModal() {
    document.getElementById('configModal').classList.remove('active');
    configNameForEdit = null;
}

async function loadConfigForEdit(name) {
    const data = await apiRequest(`/api/configs/${name}`);
    if (!data || !data.success) return;

    const config = data.config;
    document.getElementById('configName').value = name;
    
    document.getElementById('sourceHost').value = config.source.host;
    document.getElementById('sourcePort').value = config.source.port;
    document.getElementById('sourceUser').value = config.source.user;
    document.getElementById('sourcePassword').value = config.source.password;
    document.getElementById('sourceDatabase').value = config.source.database;
    
    document.getElementById('targetHost').value = config.target.host;
    document.getElementById('targetPort').value = config.target.port;
    document.getElementById('targetUser').value = config.target.user;
    document.getElementById('targetPassword').value = config.target.password;
    document.getElementById('targetDatabase').value = config.target.database;
    
    document.getElementById('syncParallel').value = config.sync.parallel_tables || 4;
    document.getElementById('syncChunkSize').value = config.sync.chunk_size || 50000;
    document.getElementById('syncBatchSize').value = config.sync.batch_insert_size || 5000;
    document.getElementById('syncTables').value = (config.sync.tables || []).join(', ');
}

async function saveConfig() {
    const name = document.getElementById('configName').value.trim();
    
    if (!name) {
        showToast('请输入配置名称', 'error');
        return;
    }

    const config = {
        source: {
            host: document.getElementById('sourceHost').value.trim(),
            port: parseInt(document.getElementById('sourcePort').value),
            user: document.getElementById('sourceUser').value.trim(),
            password: document.getElementById('sourcePassword').value,
            database: document.getElementById('sourceDatabase').value.trim(),
            charset: 'utf8mb4'
        },
        target: {
            host: document.getElementById('targetHost').value.trim(),
            port: parseInt(document.getElementById('targetPort').value),
            user: document.getElementById('targetUser').value.trim(),
            password: document.getElementById('targetPassword').value,
            database: document.getElementById('targetDatabase').value.trim(),
            charset: 'utf8mb4'
        },
        sync: {
            parallel_tables: parseInt(document.getElementById('syncParallel').value),
            chunk_size: parseInt(document.getElementById('syncChunkSize').value),
            batch_insert_size: parseInt(document.getElementById('syncBatchSize').value),
            tables: selectedTables.length > 0 ? selectedTables : 
                document.getElementById('syncTables').value.split(',').map(s => s.trim()).filter(s => s)
        }
    };

    const requiredFields = [
        ['source.host', '源数据库主机'],
        ['source.user', '源数据库用户名'],
        ['source.database', '源数据库名'],
        ['target.host', '目标数据库主机'],
        ['target.user', '目标数据库用户名'],
        ['target.database', '目标数据库名']
    ];

    for (const [field, label] of requiredFields) {
        const parts = field.split('.');
        if (!config[parts[0]][parts[1]]) {
            showToast(`请填写${label}`, 'error');
            return;
        }
    }

    const data = await apiRequest('/api/configs', 'POST', { name, config });
    if (data && data.success) {
        showToast('配置保存成功', 'success');
        closeConfigModal();
        loadConfigs();
    } else if (data) {
        showToast(data.message || '保存失败', 'error');
    }
}

async function testConnection() {
    const name = document.getElementById('configName').value.trim();
    if (!name) {
        showToast('请先保存配置再测试连接', 'error');
        return;
    }

    const tempConfig = {
        source: {
            host: document.getElementById('sourceHost').value.trim(),
            port: parseInt(document.getElementById('sourcePort').value),
            user: document.getElementById('sourceUser').value.trim(),
            password: document.getElementById('sourcePassword').value,
            database: document.getElementById('sourceDatabase').value.trim()
        },
        target: {
            host: document.getElementById('targetHost').value.trim(),
            port: parseInt(document.getElementById('targetPort').value),
            user: document.getElementById('targetUser').value.trim(),
            password: document.getElementById('targetPassword').value,
            database: document.getElementById('targetDatabase').value.trim()
        }
    };

    showToast('正在测试连接...', 'info');

    try {
        let allSuccess = true;
        const results = {};

        for (const section of ['source', 'target']) {
            try {
                const { createConnection } = await import('/static/js/db-test.js');
                const result = await createConnection(tempConfig[section]);
                results[section] = { success: result.success, message: result.message };
                if (!result.success) allSuccess = false;
            } catch (e) {
                results[section] = { success: false, message: '测试失败' };
                allSuccess = false;
            }
        }

        if (allSuccess) {
            showToast('所有连接测试通过', 'success');
        } else {
            const errors = Object.entries(results)
                .filter(([_, r]) => !r.success)
                .map(([k, r]) => `${k === 'source' ? '源' : '目标'}: ${r.message}`)
                .join('; ');
            showToast(errors, 'error');
        }
    } catch (error) {
        showToast('请先保存配置，然后使用配置列表中的测试按钮', 'info');
    }
}

async function testConfigConnection(name) {
    const data = await apiRequest(`/api/configs/${name}/test`, 'POST');
    if (data && data.success) {
        showToast('所有连接测试通过', 'success');
    } else if (data) {
        const errors = Object.entries(data.results || {})
            .filter(([_, r]) => !r.success)
            .map(([k, r]) => `${k === 'source' ? '源' : '目标'}: ${r.message}`)
            .join('; ');
        showToast(errors || '连接测试失败', 'error');
    }
}

async function loadSourceTables() {
    const host = document.getElementById('sourceHost').value.trim();
    const port = parseInt(document.getElementById('sourcePort').value);
    const user = document.getElementById('sourceUser').value.trim();
    const password = document.getElementById('sourcePassword').value;
    const database = document.getElementById('sourceDatabase').value.trim();

    if (!host || !user || !database) {
        showToast('请先填写源数据库连接信息', 'error');
        return;
    }

    const btn = document.getElementById('loadTablesBtn');
    btn.textContent = '加载中...';
    btn.disabled = true;

    const tempConfigName = '_temp_test_config_' + Date.now();
    const config = {
        source: { host, port, user, password, database, charset: 'utf8mb4' },
        target: { host: '', port: 3306, user: '', password: '', database: '', charset: 'utf8mb4' },
        sync: { parallel_tables: 1, chunk_size: 1000, batch_insert_size: 100, tables: [] }
    };

    const saveResult = await apiRequest('/api/configs', 'POST', { name: tempConfigName, config });
    
    if (saveResult && saveResult.success) {
        const tablesData = await apiRequest(`/api/configs/${tempConfigName}/tables`);
        await apiRequest(`/api/configs/${tempConfigName}`, 'DELETE');
        
        if (tablesData && tablesData.success) {
            renderTablesCheckboxes(tablesData.tables);
        } else {
            showToast(tablesData?.message || '获取表列表失败', 'error');
        }
    } else {
        showToast('请先填写完整的源数据库信息', 'error');
    }

    btn.textContent = '从源数据库加载表列表';
    btn.disabled = false;
}

function renderTablesCheckboxes(tables) {
    const container = document.getElementById('tablesCheckboxes');
    container.style.display = 'block';
    
    const currentTables = document.getElementById('syncTables').value
        .split(',')
        .map(s => s.trim())
        .filter(s => s);

    selectedTables = [...currentTables];

    container.innerHTML = `
        <div style="margin-bottom: 8px;">
            <label class="table-checkbox-item">
                <input type="checkbox" id="selectAllTables" onchange="toggleAllTables(this)" ${selectedTables.length === tables.length ? 'checked' : ''}>
                <strong>全选</strong>
            </label>
        </div>
        ${tables.map(table => `
            <label class="table-checkbox-item">
                <input type="checkbox" value="${table}" onchange="toggleTable(this)" ${selectedTables.includes(table) ? 'checked' : ''}>
                ${table}
            </label>
        `).join('')}
    `;
}

function toggleTable(checkbox) {
    const table = checkbox.value;
    if (checkbox.checked) {
        if (!selectedTables.includes(table)) {
            selectedTables.push(table);
        }
    } else {
        selectedTables = selectedTables.filter(t => t !== table);
    }
    document.getElementById('syncTables').value = selectedTables.join(', ');
}

function toggleAllTables(checkbox) {
    const checkboxes = document.querySelectorAll('#tablesCheckboxes input[type="checkbox"]:not(#selectAllTables)');
    checkboxes.forEach(cb => {
        cb.checked = checkbox.checked;
        toggleTable(cb);
    });
}

async function editConfig(name) {
    showConfigModal(name);
}

async function deleteConfig(name) {
    if (!confirm(`确定要删除配置 "${name}" 吗？`)) return;

    const data = await apiRequest(`/api/configs/${name}`, 'DELETE');
    if (data && data.success) {
        showToast('配置删除成功', 'success');
        loadConfigs();
    }
}

function createTaskFromConfig(name) {
    showTaskModal(name);
}

async function loadTasks() {
    const data = await apiRequest('/api/tasks');
    if (!data || !data.success) return;

    const container = document.getElementById('tasksList');
    
    if (data.tasks.length === 0) {
        container.innerHTML = `
            <div class="empty-state">
                <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                    <path d="M12 2v4M12 18v4M4.93 4.93l2.83 2.83M16.24 16.24l2.83 2.83M2 12h4M18 12h4M4.93 19.07l2.83-2.83M16.24 7.76l2.83-2.83"></path>
                </svg>
                <h4>暂无任务</h4>
                <p>创建您的第一个同步任务</p>
            </div>
        `;
        return;
    }

    container.innerHTML = data.tasks.map(task => `
        <div class="task-card">
            <div class="task-header">
                <div class="task-title">
                    <h4>${task.config_name}</h4>
                    <span>${getModeLabel(task.mode)} · ${formatDate(task.created_at)}</span>
                </div>
                <div class="task-actions">
                    ${task.status === 'running' || task.status === 'pending' ? `
                        <button class="icon-btn" onclick="stopTask('${task.task_id}')" title="停止">
                            <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                                <rect x="6" y="6" width="12" height="12" rx="2"></rect>
                            </svg>
                        </button>
                    ` : ''}
                    ${task.status !== 'running' ? `
                        <button class="icon-btn" onclick="restartTask('${task.task_id}')" title="重新运行">
                            <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                                <polyline points="1 4 1 10 7 10"></polyline>
                                <path d="M3.51 15a9 9 0 1 0 2.13-9.36L1 10"></path>
                            </svg>
                        </button>
                    ` : ''}
                    <button class="icon-btn" onclick="viewTaskDetail('${task.task_id}')" title="查看详情">
                        <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                            <path d="M1 12s4-8 11-8 11 8 11 8-4 8-11 8-11-8-11-8z"></path>
                            <circle cx="12" cy="12" r="3"></circle>
                        </svg>
                    </button>
                </div>
            </div>
            <div class="task-details">
                <div class="detail-item">
                    <span class="label">状态</span>
                    <span class="value">${getStatusBadge(task.status)}</span>
                </div>
                <div class="detail-item">
                    <span class="label">消息</span>
                    <span class="value" style="font-family: inherit;">${task.message || '-'}</span>
                </div>
                <div class="detail-item">
                    <span class="label">开始时间</span>
                    <span class="value" style="font-family: inherit;">${formatDate(task.started_at)}</span>
                </div>
                <div class="detail-item">
                    <span class="label">结束时间</span>
                    <span class="value" style="font-family: inherit;">${formatDate(task.finished_at)}</span>
                </div>
            </div>
        </div>
    `).join('');
}

function showTaskModal(configName = null) {
    const modal = document.getElementById('taskModal');
    const select = document.getElementById('taskConfigSelect');
    
    select.innerHTML = '<option value="">请选择配置...</option>';
    
    apiRequest('/api/configs').then(data => {
        if (data && data.success) {
            data.configs.forEach(config => {
                const option = document.createElement('option');
                option.value = config.name;
                option.textContent = config.name;
                if (configName === config.name) {
                    option.selected = true;
                }
                select.appendChild(option);
            });
        }
    });

    modal.classList.add('active');
}

function closeTaskModal() {
    document.getElementById('taskModal').classList.remove('active');
}

async function createTask() {
    const configName = document.getElementById('taskConfigSelect').value;
    if (!configName) {
        showToast('请选择配置', 'error');
        return;
    }

    const mode = document.querySelector('input[name="syncMode"]:checked').value;

    const data = await apiRequest('/api/tasks', 'POST', { config_name: configName, mode });
    if (data && data.success) {
        showToast('任务创建成功，正在启动...', 'success');
        closeTaskModal();
        
        const taskId = data.task.task_id;
        const startData = await apiRequest(`/api/tasks/${taskId}/start`, 'POST');
        
        if (startData && startData.success) {
            showToast('任务已启动', 'success');
            loadTasks();
            viewTaskDetail(taskId);
        }
    }
}

async function stopTask(taskId) {
    if (!confirm('确定要停止此任务吗？')) return;

    const data = await apiRequest(`/api/tasks/${taskId}/stop`, 'POST');
    if (data && data.success) {
        showToast('正在停止任务...', 'info');
        setTimeout(() => loadTasks(), 2000);
    }
}

async function restartTask(taskId) {
    const taskData = await apiRequest(`/api/tasks/${taskId}/progress`);
    if (!taskData || !taskData.success) return;

    const task = taskData.progress.task;
    const mode = task.mode === 'restart' ? 'restart' : 'resume';
    
    const newData = await apiRequest('/api/tasks', 'POST', { 
        config_name: task.config_name, 
        mode 
    });
    
    if (newData && newData.success) {
        const newTaskId = newData.task.task_id;
        await apiRequest(`/api/tasks/${newTaskId}/start`, 'POST');
        showToast('任务已重新启动', 'success');
        loadTasks();
        viewTaskDetail(newTaskId);
    }
}

async function viewTaskDetail(taskId) {
    currentTaskId = taskId;
    const modal = document.getElementById('taskDetailModal');
    const content = document.getElementById('taskDetailContent');

    modal.classList.add('active');

    await refreshTaskDetail(taskId);

    if (progressPollingInterval) {
        clearInterval(progressPollingInterval);
    }
    
    progressPollingInterval = setInterval(() => {
        if (document.getElementById('taskDetailModal').classList.contains('active')) {
            refreshTaskDetail(taskId);
        } else {
            clearInterval(progressPollingInterval);
            progressPollingInterval = null;
        }
    }, 2000);
}

async function refreshTaskDetail(taskId) {
    const data = await apiRequest(`/api/tasks/${taskId}/progress`);
    if (!data || !data.success) return;

    const { task, tables, overall } = data.progress;
    const content = document.getElementById('taskDetailContent');

    document.getElementById('taskDetailTitle').textContent = `${task.config_name} - 任务详情`;

    let tablesHtml = '';
    if (tables.length > 0) {
        tablesHtml = tables.map(t => {
            const pct = t.total_rows > 0 ? Math.round((t.synced_rows / t.total_rows) * 100) : 0;
            let barClass = '';
            if (t.status === 'done') barClass = 'success';
            if (t.status === 'failed') barClass = 'danger';
            
            return `
                <div class="table-progress-item">
                    <div class="table-name">${t.table_name}</div>
                    <div class="table-bar">
                        <div class="progress-bar">
                            <div class="progress-bar-fill ${barClass}" style="width: ${pct}%"></div>
                        </div>
                    </div>
                    <div class="table-stats">
                        ${getStatusBadge(t.status)} ${formatNumber(t.synced_rows)}/${formatNumber(t.total_rows)}
                    </div>
                </div>
            `;
        }).join('');
    } else {
        tablesHtml = '<p style="color: var(--text-secondary); text-align: center; padding: 20px;">暂无进度数据</p>';
    }

    content.innerHTML = `
        <div style="margin-bottom: 24px;">
            <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 16px;">
                <div>
                    <h3 style="margin-bottom: 8px;">总体进度</h3>
                    <p style="color: var(--text-secondary);">${task.message || ''}</p>
                </div>
                ${getStatusBadge(task.status)}
            </div>
            <div class="progress-bar" style="height: 12px;">
                <div class="progress-bar-fill ${task.status === 'completed' ? 'success' : ''}" style="width: ${overall.percentage}%"></div>
            </div>
            <div style="display: flex; justify-content: space-between; margin-top: 12px; font-size: 14px; color: var(--text-secondary);">
                <span>进度: ${overall.percentage}%</span>
                <span>表: ${overall.completed_tables}/${overall.total_tables}</span>
                <span>行数: ${formatNumber(overall.synced_rows)}/${formatNumber(overall.total_rows)}</span>
            </div>
        </div>
        
        <div style="border-top: 1px solid var(--border-color); padding-top: 20px;">
            <h4 style="margin-bottom: 16px;">各表同步进度</h4>
            ${tablesHtml}
        </div>
        
        <div style="border-top: 1px solid var(--border-color); padding-top: 20px; margin-top: 20px;">
            <h4 style="margin-bottom: 16px;">任务信息</h4>
            <div class="config-details">
                <div class="detail-item">
                    <span class="label">配置名称</span>
                    <span class="value" style="font-family: inherit;">${task.config_name}</span>
                </div>
                <div class="detail-item">
                    <span class="label">同步模式</span>
                    <span class="value" style="font-family: inherit;">${getModeLabel(task.mode)}</span>
                </div>
                <div class="detail-item">
                    <span class="label">创建时间</span>
                    <span class="value" style="font-family: inherit;">${formatDate(task.created_at)}</span>
                </div>
                <div class="detail-item">
                    <span class="label">开始时间</span>
                    <span class="value" style="font-family: inherit;">${formatDate(task.started_at)}</span>
                </div>
                <div class="detail-item">
                    <span class="label">结束时间</span>
                    <span class="value" style="font-family: inherit;">${formatDate(task.finished_at)}</span>
                </div>
            </div>
        </div>
    `;
}

function closeTaskDetailModal() {
    document.getElementById('taskDetailModal').classList.remove('active');
    if (progressPollingInterval) {
        clearInterval(progressPollingInterval);
        progressPollingInterval = null;
    }
    currentTaskId = null;
}

async function loadProgress() {
    const data = await apiRequest('/api/dashboard');
    if (!data || !data.success) return;

    const progressList = data.active_progress || [];
    const container = document.getElementById('progressList');

    if (progressList.length === 0) {
        container.innerHTML = `
            <div class="empty-state">
                <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                    <line x1="12" y1="20" x2="12" y2="10"></line>
                    <polyline points="6 20 12 10 18 20"></polyline>
                    <path d="M4 4h16"></path>
                </svg>
                <h4>暂无断点数据</h4>
                <p>运行同步任务后将显示断点信息</p>
            </div>
        `;
        return;
    }

    container.innerHTML = progressList.map(p => {
        const pct = p.total_rows > 0 ? Math.round((p.synced_rows / p.total_rows) * 100) : 0;
        let barClass = '';
        if (p.status === 'done') barClass = 'success';
        if (p.status === 'failed') barClass = 'danger';

        return `
            <div class="progress-card">
                <div class="progress-header">
                    <div class="progress-title">
                        <h4>${p.table_name}</h4>
                        <span>主键: ${p.primary_key} · 最后位置: ${p.last_pk_value}</span>
                    </div>
                    <div class="progress-actions">
                        ${p.status !== 'done' ? `
                            <button class="icon-btn danger" onclick="resetTableProgress('${p.table_name}')" title="重置此表进度">
                                <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                                    <polyline points="1 4 1 10 7 10"></polyline>
                                    <path d="M3.51 15a9 9 0 1 0 2.13-9.36L1 10"></path>
                                </svg>
                            </button>
                        ` : ''}
                    </div>
                </div>
                <div class="progress-bar-container">
                    <div class="progress-bar">
                        <div class="progress-bar-fill ${barClass}" style="width: ${pct}%"></div>
                    </div>
                    <div class="progress-info">
                        <span>${getStatusBadge(p.status)}</span>
                        <span>${formatNumber(p.synced_rows)} / ${formatNumber(p.total_rows)} 行 (${pct}%)</span>
                        <span>更新于: ${formatDate(p.updated_at)}</span>
                    </div>
                </div>
            </div>
        `;
    }).join('');
}

async function resetTableProgress(tableName) {
    if (!confirm(`确定要重置表 "${tableName}" 的同步进度吗？`)) return;

    const data = await apiRequest('/api/progress/reset', 'POST', { table_name: tableName });
    if (data && data.success) {
        showToast('进度已重置', 'success');
        loadProgress();
    }
}

async function resetAllProgress() {
    if (!confirm('确定要重置所有同步进度吗？这将清除所有断点信息。')) return;

    const data = await apiRequest('/api/progress/reset', 'POST', {});
    if (data && data.success) {
        showToast('所有进度已重置', 'success');
        loadProgress();
    }
}

document.addEventListener('click', function(e) {
    if (e.target.classList.contains('modal')) {
        e.target.classList.remove('active');
    }
});

document.addEventListener('keydown', function(e) {
    if (e.key === 'Escape') {
        document.querySelectorAll('.modal.active').forEach(m => m.classList.remove('active'));
        closeTaskDetailModal();
    }
});

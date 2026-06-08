let currentTab = 'dashboard';
let currentTaskId = null;
let progressPollingInterval = null;
let configNameForEdit = null;
let selectedTables = [];

/** 数据库类型对应的默认端口 */
const DB_DEFAULT_PORTS = {
    mysql: 3306,
    db2: 50000,
    oracle: 1521
};

/** 数据库类型对应的标签 */
const DB_TYPE_LABELS = {
    mysql: 'MySQL',
    db2: 'DB2',
    oracle: 'Oracle'
};

const pageTitles = {
    dashboard: { title: '总览仪表盘', subtitle: '查看数据同步平台的整体运行状态' },
    resources: { title: '资源管理', subtitle: '管理所有数据库连接资源，作为同步任务的基础' },
    canvas: { title: '同步画布', subtitle: '可视化拖拽建立同步关系' },
    pipelines: { title: '同步流水线', subtitle: '管理所有已配置的同步流水线' },
    tasks: { title: '任务监控', subtitle: '监控和管理同步任务' },
    progress: { title: '断点续传', subtitle: '管理同步断点和进度' },
    realtime: { title: '实时同步', subtitle: '管理实时 CDC 数据同步' }
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
    const tabEl = document.getElementById(`tab-${tab}`);
    if (tabEl) tabEl.classList.add('active');

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
    if (tab === 'realtime') loadRealtimeTasks();
    if (tab === 'resources') loadResources();
    if (tab === 'canvas') loadCanvas();
    if (tab === 'pipelines') loadPipelines();
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

/**
 * 数据库类型切换处理。
 * 当用户在源/目标数据库选择不同的类型时，自动更新默认端口。
 * @param {string} section - "source" 或 "target"
 */
function onDbTypeChange(section) {
    const typeSelect = document.getElementById(`${section}Type`);
    const portInput = document.getElementById(`${section}Port`);
    const selectedType = typeSelect.value;

    // 仅当端口是当前类型的默认值时，才自动切换端口
    const currentPort = parseInt(portInput.value);
    const isDefaultPort = Object.values(DB_DEFAULT_PORTS).includes(currentPort);
    if (isDefaultPort) {
        portInput.value = DB_DEFAULT_PORTS[selectedType] || 3306;
    }

    console.log(`${section} 数据库类型切换为: ${selectedType}, 端口: ${portInput.value}`);
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
                    <span class="value">${DB_TYPE_LABELS[config.config.source.type] || 'MySQL'}: ${config.config.source.host}:${config.config.source.port}/${config.config.source.database}</span>
                </div>
                <div class="detail-item">
                    <span class="label">目标数据库</span>
                    <span class="value">${DB_TYPE_LABELS[config.config.target.type] || 'MySQL'}: ${config.config.target.host}:${config.config.target.port}/${config.config.target.database}</span>
                </div>
                <div class="detail-item">
                    <span class="label">同步方向</span>
                    <span class="value">${DB_TYPE_LABELS[config.config.source.type] || 'MySQL'} → ${DB_TYPE_LABELS[config.config.target.type] || 'MySQL'}</span>
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
        document.getElementById('sourceType').value = 'mysql';
        document.getElementById('targetType').value = 'mysql';
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
    
    document.getElementById('sourceType').value = config.source.type || 'mysql';
    document.getElementById('sourceHost').value = config.source.host;
    document.getElementById('sourcePort').value = config.source.port;
    document.getElementById('sourceUser').value = config.source.user;
    document.getElementById('sourcePassword').value = config.source.password;
    document.getElementById('sourceDatabase').value = config.source.database;
    
    document.getElementById('targetType').value = config.target.type || 'mysql';
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
    try {
        const name = document.getElementById('configName').value.trim();
        
        if (!name) {
            showToast('请输入配置名称', 'error');
            return;
        }

        const sourceType = document.getElementById('sourceType').value;
        const targetType = document.getElementById('targetType').value;

        const sourceServiceName = document.getElementById('sourceServiceName')?.value?.trim() || '';
        const sourceSid = document.getElementById('sourceSid')?.value?.trim() || '';
        const targetServiceName = document.getElementById('targetServiceName')?.value?.trim() || '';
        const targetSid = document.getElementById('targetSid')?.value?.trim() || '';

        const config = {
            source: {
                type: sourceType,
                host: document.getElementById('sourceHost').value.trim(),
                port: parseInt(document.getElementById('sourcePort').value),
                user: document.getElementById('sourceUser').value.trim(),
                password: document.getElementById('sourcePassword').value,
                database: document.getElementById('sourceDatabase').value.trim(),
                charset: 'utf8mb4'
            },
            target: {
                type: targetType,
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

        console.log('保存配置:', name, '源:', sourceType, '目标:', targetType);
        const data = await apiRequest('/api/configs', 'POST', { name, config });
        if (data && data.success) {
            showToast('配置保存成功', 'success');
            closeConfigModal();
            loadConfigs();
        } else if (data) {
            showToast(data.message || '保存失败', 'error');
        } else {
            showToast('保存失败，请检查网络连接', 'error');
        }
    } catch (error) {
        console.error('saveConfig 错误:', error);
        showToast('保存配置时出错: ' + error.message, 'error');
    }
}

async function testConnection() {
    const name = document.getElementById('configName').value.trim();
    if (!name) {
        showToast('请先输入配置名称', 'error');
        return;
    }

    const sourceType = document.getElementById('sourceType').value;
    const targetType = document.getElementById('targetType').value;
    const sourceHost = document.getElementById('sourceHost').value.trim();
    const sourceUser = document.getElementById('sourceUser').value.trim();
    const sourceDatabase = document.getElementById('sourceDatabase').value.trim();
    const targetHost = document.getElementById('targetHost').value.trim();
    const targetUser = document.getElementById('targetUser').value.trim();
    const targetDatabase = document.getElementById('targetDatabase').value.trim();

    if (!sourceHost || !sourceUser || !sourceDatabase) {
        showToast('请先填写源数据库连接信息', 'error');
        return;
    }
    if (!targetHost || !targetUser || !targetDatabase) {
        showToast('请先填写目标数据库连接信息', 'error');
        return;
    }

    showToast('正在测试连接...', 'info');

    const tempConfig = {
        source: {
            type: sourceType,
            host: sourceHost,
            port: parseInt(document.getElementById('sourcePort').value),
            user: sourceUser,
            password: document.getElementById('sourcePassword').value,
            database: sourceDatabase,
            charset: 'utf8mb4'
        },
        target: {
            type: targetType,
            host: targetHost,
            port: parseInt(document.getElementById('targetPort').value),
            user: targetUser,
            password: document.getElementById('targetPassword').value,
            database: targetDatabase,
            charset: 'utf8mb4'
        },
        sync: { parallel_tables: 1, chunk_size: 1000, batch_insert_size: 100, tables: [] }
    };

    const tempConfigName = '_temp_test_' + Date.now();

    try {
        const saveResult = await apiRequest('/api/configs', 'POST', { name: tempConfigName, config: tempConfig });
        if (!saveResult || !saveResult.success) {
            showToast('请先填写完整的数据库连接信息', 'error');
            return;
        }

        const testResult = await apiRequest(`/api/configs/${tempConfigName}/test`, 'POST');
        await apiRequest(`/api/configs/${tempConfigName}`, 'DELETE');

        if (testResult && testResult.success) {
            showToast('所有连接测试通过', 'success');
        } else if (testResult) {
            const errors = Object.entries(testResult.results || {})
                .filter(([_, r]) => !r.success)
                .map(([k, r]) => `${k === 'source' ? '源' : '目标'}: ${r.message}`)
                .join('; ');
            showToast(errors || '连接测试失败', 'error');
        } else {
            showToast('连接测试请求失败', 'error');
        }
    } catch (error) {
        console.error('testConnection 错误:', error);
        showToast('测试连接时出错: ' + error.message, 'error');
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
    const sourceType = document.getElementById('sourceType').value;

    if (!host || !user || !database) {
        showToast('请先填写源数据库连接信息', 'error');
        return;
    }

    const btn = document.getElementById('loadTablesBtn');
    btn.textContent = '加载中...';
    btn.disabled = true;

    const tempConfigName = '_temp_test_config_' + Date.now();
    const config = {
        source: { type: sourceType, host, port, user, password, database, charset: 'utf8mb4' },
        target: { type: 'mysql', host: '', port: 3306, user: '', password: '', database: '', charset: 'utf8mb4' },
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

    try {
        const data = await apiRequest('/api/tasks', 'POST', { config_name: configName, mode });
        if (data && data.success) {
            showToast('任务创建成功，正在启动...', 'success');
            closeTaskModal();
            
            const taskId = data.task.task_id;
            const startData = await apiRequest(`/api/tasks/${taskId}/start`, 'POST');
            
            if (startData && startData.success) {
                showToast('任务已启动', 'success');
                switchTab('tasks');
                loadTasks();
                setTimeout(() => viewTaskDetail(taskId), 500);
            } else {
                showToast(startData?.message || '任务启动失败', 'error');
                switchTab('tasks');
                loadTasks();
            }
        } else {
            showToast(data?.message || '创建任务失败', 'error');
        }
    } catch (error) {
        console.error('createTask 错误:', error);
        showToast('创建任务时出错: ' + error.message, 'error');
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
        closeRealtimeDetailModal();
    }
});

// ====================================================================== //
//                        实时同步功能
// ====================================================================== //

let realtimePollingInterval = null;
let currentRealtimeTaskId = null;

async function loadRealtimeTasks() {
    const data = await apiRequest('/api/realtime/dashboard');
    if (!data || !data.success) return;

    const stats = data.stats;
    document.getElementById('realtimeTotalTasks').textContent = stats.total_tasks;
    document.getElementById('realtimeRunning').textContent = stats.running_tasks;
    document.getElementById('realtimeTotalEvents').textContent = formatNumber(stats.total_events);
    document.getElementById('realtimeSyncedEvents').textContent = formatNumber(stats.synced_events);

    const container = document.getElementById('realtimeTasksList');
    const tasks = data.recent_tasks || [];

    if (tasks.length === 0) {
        container.innerHTML = `
            <div class="empty-state">
                <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                    <circle cx="12" cy="12" r="10"></circle>
                    <polyline points="12 6 12 12 16 14"></polyline>
                </svg>
                <h4>暂无实时任务</h4>
                <p>创建您的第一个实时同步任务</p>
            </div>
        `;
        return;
    }

    container.innerHTML = tasks.map(task => `
        <div class="task-card">
            <div class="task-header">
                <div class="task-title">
                    <h4>${task.config_name}</h4>
                    <span>创建时间: ${formatDate(task.created_at)}</span>
                </div>
                <div class="task-actions">
                    ${task.status === 'running' ? `
                        <button class="icon-btn" onclick="stopRealtimeTask('${task.task_id}')" title="停止">
                            <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                                <rect x="6" y="6" width="12" height="12" rx="2"></rect>
                            </svg>
                        </button>
                    ` : `
                        <button class="icon-btn" onclick="startRealtimeTask('${task.task_id}')" title="启动">
                            <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                                <polygon points="5 3 19 12 5 21 5 3"></polygon>
                            </svg>
                        </button>
                    `}
                    <button class="icon-btn" onclick="viewRealtimeTaskDetail('${task.task_id}')" title="查看详情">
                        <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                            <path d="M1 12s4-8 11-8 11 8 11 8-4 8-11 8-11-8-11-8z"></path>
                            <circle cx="12" cy="12" r="3"></circle>
                        </svg>
                    </button>
                    <button class="icon-btn danger" onclick="deleteRealtimeTask('${task.task_id}')" title="删除">
                        <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                            <polyline points="3 6 5 6 21 6"></polyline>
                            <path d="M19 6v14a2 2 0 0 1-2 2H7a2 2 0 0 1-2-2V6m3 0V4a2 2 0 0 1 2-2h4a2 2 0 0 1 2 2v2"></path>
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
                    <span class="label">总事件数</span>
                    <span class="value">${formatNumber(task.stats?.total_events || 0)}</span>
                </div>
                <div class="detail-item">
                    <span class="label">已同步</span>
                    <span class="value">${formatNumber(task.stats?.synced_events || 0)}</span>
                </div>
                ${task.binlog_file ? `
                <div class="detail-item">
                    <span class="label">Binlog 位置</span>
                    <span class="value" style="font-family: inherit;">${task.binlog_file}:${task.binlog_pos}</span>
                </div>
                ` : ''}
            </div>
        </div>
    `).join('');
}

function showRealtimeTaskModal() {
    const modal = document.getElementById('realtimeTaskModal');
    const select = document.getElementById('realtimeConfigSelect');
    
    select.innerHTML = '<option value="">请选择配置...</option>';
    
    apiRequest('/api/configs').then(data => {
        if (data && data.success) {
            data.configs.forEach(config => {
                const srcType = config.config.source?.type || 'mysql';
                if (srcType === 'mysql') {
                    const option = document.createElement('option');
                    option.value = config.name;
                    option.textContent = config.name;
                    select.appendChild(option);
                }
            });
        }
    });

    modal.classList.add('active');
}

function closeRealtimeTaskModal() {
    document.getElementById('realtimeTaskModal').classList.remove('active');
}

async function createRealtimeTask() {
    const configName = document.getElementById('realtimeConfigSelect').value;
    if (!configName) {
        showToast('请选择配置', 'error');
        return;
    }

    const mode = document.querySelector('input[name="realtimeMode"]:checked').value;
    const resumeFromBinlog = mode === 'resume';

    try {
        const createData = await apiRequest('/api/realtime/tasks', 'POST', { config_name: configName });
        if (!createData || !createData.success) {
            showToast(createData?.message || '创建实时任务失败', 'error');
            return;
        }

        const taskId = createData.task.task_id;
        const startData = await apiRequest(`/api/realtime/tasks/${taskId}/start`, 'POST', {
            resume_from_binlog: resumeFromBinlog
        });

        if (startData && startData.success) {
            showToast('实时任务已启动', 'success');
            closeRealtimeTaskModal();
            loadRealtimeTasks();
            setTimeout(() => viewRealtimeTaskDetail(taskId), 500);
        } else {
            showToast(startData?.message || '启动实时任务失败', 'error');
            loadRealtimeTasks();
        }
    } catch (error) {
        console.error('createRealtimeTask 错误:', error);
        showToast('创建实时任务时出错: ' + error.message, 'error');
    }
}

async function startRealtimeTask(taskId) {
    const data = await apiRequest(`/api/realtime/tasks/${taskId}/start`, 'POST', {
        resume_from_binlog: true
    });
    if (data && data.success) {
        showToast('实时任务已启动', 'success');
        loadRealtimeTasks();
    } else {
        showToast(data?.message || '启动失败', 'error');
    }
}

async function stopRealtimeTask(taskId) {
    if (!confirm('确定要停止此实时任务吗？')) return;

    const data = await apiRequest(`/api/realtime/tasks/${taskId}/stop`, 'POST');
    if (data && data.success) {
        showToast('正在停止实时任务...', 'info');
        setTimeout(() => loadRealtimeTasks(), 2000);
    }
}

async function deleteRealtimeTask(taskId) {
    if (!confirm('确定要删除此实时任务吗？')) return;

    const data = await apiRequest(`/api/realtime/tasks/${taskId}`, 'DELETE');
    if (data && data.success) {
        showToast('实时任务已删除', 'success');
        loadRealtimeTasks();
    }
}

async function viewRealtimeTaskDetail(taskId) {
    currentRealtimeTaskId = taskId;
    const modal = document.getElementById('realtimeDetailModal');
    const content = document.getElementById('realtimeDetailContent');

    modal.classList.add('active');

    await refreshRealtimeTaskDetail(taskId);

    if (realtimePollingInterval) {
        clearInterval(realtimePollingInterval);
    }
    
    realtimePollingInterval = setInterval(() => {
        if (document.getElementById('realtimeDetailModal').classList.contains('active')) {
            refreshRealtimeTaskDetail(taskId);
        } else {
            clearInterval(realtimePollingInterval);
            realtimePollingInterval = null;
        }
    }, 2000);
}

async function refreshRealtimeTaskDetail(taskId) {
    const data = await apiRequest(`/api/realtime/tasks/${taskId}/stats`);
    if (!data || !data.success) return;

    const stats = data.stats;
    const content = document.getElementById('realtimeDetailContent');

    document.getElementById('realtimeDetailTitle').textContent = `实时任务详情 - ${taskId.substring(0, 8)}...`;

    content.innerHTML = `
        <div style="margin-bottom: 24px;">
            <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 16px;">
                <div>
                    <h3 style="margin-bottom: 8px;">同步状态</h3>
                    <p style="color: var(--text-secondary);">${stats.message || ''}</p>
                </div>
                ${getStatusBadge(stats.status)}
            </div>
        </div>

        <div class="config-details" style="margin-bottom: 24px;">
            <div class="detail-item">
                <span class="label">总事件数</span>
                <span class="value">${formatNumber(stats.total_events || 0)}</span>
            </div>
            <div class="detail-item">
                <span class="label">INSERT 事件</span>
                <span class="value">${formatNumber(stats.insert_events || 0)}</span>
            </div>
            <div class="detail-item">
                <span class="label">UPDATE 事件</span>
                <span class="value">${formatNumber(stats.update_events || 0)}</span>
            </div>
            <div class="detail-item">
                <span class="label">DELETE 事件</span>
                <span class="value">${formatNumber(stats.delete_events || 0)}</span>
            </div>
            <div class="detail-item">
                <span class="label">已同步</span>
                <span class="value" style="color: var(--success-color);">${formatNumber(stats.synced_events || 0)}</span>
            </div>
            <div class="detail-item">
                <span class="label">失败事件</span>
                <span class="value" style="color: var(--danger-color);">${formatNumber(stats.failed_events || 0)}</span>
            </div>
            ${stats.queue_size !== undefined ? `
            <div class="detail-item">
                <span class="label">队列大小</span>
                <span class="value">${stats.queue_size}</span>
            </div>
            ` : ''}
            ${stats.running_time !== undefined ? `
            <div class="detail-item">
                <span class="label">运行时间</span>
                <span class="value">${formatDuration(stats.running_time)}</span>
            </div>
            ` : ''}
        </div>

        ${stats.binlog_file ? `
        <div style="border-top: 1px solid var(--border-color); padding-top: 20px;">
            <h4 style="margin-bottom: 16px;">Binlog 位置</h4>
            <div class="config-details">
                <div class="detail-item">
                    <span class="label">日志文件</span>
                    <span class="value" style="font-family: monospace;">${stats.binlog_file}</span>
                </div>
                <div class="detail-item">
                    <span class="label">位置</span>
                    <span class="value" style="font-family: monospace;">${stats.binlog_pos}</span>
                </div>
            </div>
        </div>
        ` : ''}
    `;
}

function formatDuration(seconds) {
    if (!seconds || seconds < 0) return '-';
    const h = Math.floor(seconds / 3600);
    const m = Math.floor((seconds % 3600) / 60);
    const s = Math.floor(seconds % 60);
    if (h > 0) return `${h}时${m}分${s}秒`;
    if (m > 0) return `${m}分${s}秒`;
    return `${s}秒`;
}

function closeRealtimeDetailModal() {
    document.getElementById('realtimeDetailModal').classList.remove('active');
    if (realtimePollingInterval) {
        clearInterval(realtimePollingInterval);
        realtimePollingInterval = null;
    }
    currentRealtimeTaskId = null;
}

// ====================================================================== //
//                           资源管理功能
// ====================================================================== //

let currentResourceId = null;

async function loadResources() {
    const data = await apiRequest('/api/resources');
    if (!data || !data.success) return;

    const resources = data.resources || [];
    const container = document.getElementById('resourceGrid');
    const countEl = document.getElementById('resourceCount');
    
    if (countEl) countEl.textContent = resources.length;

    if (resources.length === 0) {
        container.innerHTML = `
            <div class="empty-state" style="grid-column: 1 / -1;">
                <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                    <ellipse cx="12" cy="5" rx="9" ry="3"></ellipse>
                    <path d="M21 12c0 1.66-4 3-9 3s-9-1.34-9-3"></path>
                    <path d="M3 5v14c0 1.66 4 3 9 3s9-1.34 9-3V5"></path>
                </svg>
                <h4>暂无资源</h4>
                <p>添加您的第一个数据库资源开始使用</p>
                <button class="btn-primary" onclick="showResourceModal()">添加资源</button>
            </div>
        `;
        return;
    }

    container.innerHTML = resources.map(resource => `
        <div class="resource-card" data-resource-id="${resource.resource_id}">
            <div class="resource-card-header">
                <div class="resource-icon ${resource.type}">
                    <svg width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                        <ellipse cx="12" cy="5" rx="9" ry="3"></ellipse>
                        <path d="M21 12c0 1.66-4 3-9 3s-9-1.34-9-3"></path>
                        <path d="M3 5v14c0 1.66 4 3 9 3s9-1.34 9-3V5"></path>
                    </svg>
                </div>
                <div class="resource-info">
                    <h4>${resource.name}</h4>
                    <span class="resource-type">${DB_TYPE_LABELS[resource.type] || resource.type}</span>
                </div>
            </div>
            <div class="resource-card-body">
                <div class="resource-detail-item">
                    <span class="label">主机</span>
                    <span class="value">${resource.host}:${resource.port}</span>
                </div>
                <div class="resource-detail-item">
                    <span class="label">数据库</span>
                    <span class="value">${resource.database}</span>
                </div>
                <div class="resource-detail-item">
                    <span class="label">用户</span>
                    <span class="value">${resource.user}</span>
                </div>
            </div>
            <div class="resource-card-footer">
                <button class="btn-secondary btn-sm" onclick="testResourceConnection('${resource.resource_id}')">测试连接</button>
                <button class="btn-secondary btn-sm" onclick="editResource('${resource.resource_id}')">编辑</button>
                <button class="btn-danger btn-sm" onclick="deleteResource('${resource.resource_id}')">删除</button>
            </div>
        </div>
    `).join('');
}

function showResourceModal(resourceId = null) {
    currentResourceId = resourceId;
    const modal = document.getElementById('resourceModal');
    const title = document.getElementById('resourceModalTitle');
    const form = document.getElementById('resourceForm');
    
    form.reset();
    document.querySelectorAll('.db-type-option').forEach(opt => opt.classList.remove('selected'));
    document.querySelector('.db-type-option[data-type="mysql"]').classList.add('selected');
    document.querySelector('input[name="dbType"][value="mysql"]').checked = true;
    document.getElementById('resourcePort').value = 3306;

    if (resourceId) {
        title.textContent = '编辑数据库资源';
        loadResourceForEdit(resourceId);
    } else {
        title.textContent = '添加数据库资源';
        document.getElementById('resourceName').value = '';
        document.getElementById('resourceHost').value = '';
        document.getElementById('resourceUser').value = '';
        document.getElementById('resourcePassword').value = '';
        document.getElementById('resourceDatabase').value = '';
    }
    
    modal.classList.add('active');
}

function closeResourceModal() {
    document.getElementById('resourceModal').classList.remove('active');
    currentResourceId = null;
}

async function loadResourceForEdit(resourceId) {
    const data = await apiRequest(`/api/resources/${resourceId}`);
    if (!data || !data.success) return;

    const resource = data.resource;
    document.getElementById('resourceName').value = resource.name;
    document.getElementById('resourceHost').value = resource.host;
    document.getElementById('resourcePort').value = resource.port;
    document.getElementById('resourceUser').value = resource.user;
    document.getElementById('resourcePassword').value = resource.password;
    document.getElementById('resourceDatabase').value = resource.database;

    document.querySelectorAll('.db-type-option').forEach(opt => {
        opt.classList.toggle('selected', opt.dataset.type === resource.type);
    });
    const radio = document.querySelector(`input[name="dbType"][value="${resource.type}"]`);
    if (radio) radio.checked = true;
}

document.addEventListener('click', function(e) {
    const dbTypeOption = e.target.closest('.db-type-option');
    if (dbTypeOption) {
        document.querySelectorAll('.db-type-option').forEach(opt => opt.classList.remove('selected'));
        dbTypeOption.classList.add('selected');
        const type = dbTypeOption.dataset.type;
        const radio = dbTypeOption.querySelector('input[type="radio"]');
        if (radio) radio.checked = true;
        document.getElementById('resourcePort').value = DB_DEFAULT_PORTS[type] || 3306;
    }
});

async function saveResource() {
    const name = document.getElementById('resourceName').value.trim();
    if (!name) {
        showToast('请输入资源名称', 'error');
        return;
    }

    const type = document.querySelector('input[name="dbType"]:checked')?.value || 'mysql';
    const resource = {
        name: name,
        type: type,
        host: document.getElementById('resourceHost').value.trim(),
        port: parseInt(document.getElementById('resourcePort').value) || 3306,
        user: document.getElementById('resourceUser').value.trim(),
        password: document.getElementById('resourcePassword').value,
        database: document.getElementById('resourceDatabase').value.trim(),
    };

    const requiredFields = [
        ['host', '主机地址'],
        ['user', '用户名'],
        ['database', '数据库名'],
    ];

    for (const [field, label] of requiredFields) {
        if (!resource[field]) {
            showToast(`请填写${label}`, 'error');
            return;
        }
    }

    try {
        let data;
        if (currentResourceId) {
            data = await apiRequest(`/api/resources/${currentResourceId}`, 'PUT', resource);
        } else {
            data = await apiRequest('/api/resources', 'POST', resource);
        }

        if (data && data.success) {
            showToast(currentResourceId ? '资源更新成功' : '资源创建成功', 'success');
            closeResourceModal();
            loadResources();
        } else {
            showToast(data?.message || '保存失败', 'error');
        }
    } catch (error) {
        showToast('保存资源时出错: ' + error.message, 'error');
    }
}

async function testResourceConnection(resourceId) {
    if (!resourceId) {
        const name = document.getElementById('resourceName').value.trim();
        if (!name) {
            showToast('请先输入资源名称', 'error');
            return;
        }
        showToast('请先保存资源后再测试连接', 'info');
        return;
    }

    showToast('正在测试连接...', 'info');
    const data = await apiRequest(`/api/resources/${resourceId}/test`, 'POST');
    if (data && data.success) {
        showToast('连接测试通过', 'success');
    } else {
        showToast(data?.message || '连接测试失败', 'error');
    }
}

async function editResource(resourceId) {
    showResourceModal(resourceId);
}

async function deleteResource(resourceId) {
    if (!confirm('确定要删除此资源吗？删除后将无法恢复。')) return;

    const data = await apiRequest(`/api/resources/${resourceId}`, 'DELETE');
    if (data && data.success) {
        showToast('资源删除成功', 'success');
        loadResources();
    }
}

// ====================================================================== //
//                           画布编辑器功能
// ====================================================================== //

let canvasNodes = [];
let canvasConnections = [];
let selectedNode = null;
let selectedConnection = null;
let isDragging = false;
let isConnecting = false;
let connectingFrom = null;
let pendingConnection = null;
let tempConnectionLine = null;
let dragOffset = { x: 0, y: 0 };
let nodeIdCounter = 0;
let canvasEventsInitialized = false;

function loadCanvas() {
    loadResourcePalette();
    renderCanvas();
    if (!canvasEventsInitialized) {
        initGlobalCanvasEvents();
        canvasEventsInitialized = true;
    }
}

async function loadResourcePalette() {
    const data = await apiRequest('/api/resources');
    const container = document.getElementById('resourcePalette');
    
    if (!data || !data.success || data.resources.length === 0) {
        container.innerHTML = `
            <div class="empty-state-small">
                <p>暂无可用资源</p>
                <button class="btn-link" onclick="switchTab('resources')">去添加</button>
            </div>
        `;
        return;
    }

    container.innerHTML = data.resources.map(resource => `
        <div class="palette-item" draggable="true" data-resource-id="${resource.resource_id}" data-resource-type="${resource.type}" data-resource-name="${resource.name}">
            <div class="palette-item-icon ${resource.type}">
                <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                    <ellipse cx="12" cy="5" rx="9" ry="3"></ellipse>
                    <path d="M21 12c0 1.66-4 3-9 3s-9-1.34-9-3"></path>
                    <path d="M3 5v14c0 1.66 4 3 9 3s9-1.34 9-3V5"></path>
                </svg>
            </div>
            <div class="palette-item-info">
                <span class="palette-item-name">${resource.name}</span>
                <span class="palette-item-type">${DB_TYPE_LABELS[resource.type] || resource.type}</span>
            </div>
        </div>
    `).join('');

    initPaletteDrag();
}

function initPaletteDrag() {
    document.querySelectorAll('.palette-item').forEach(item => {
        item.addEventListener('dragstart', function(e) {
            e.dataTransfer.setData('resourceId', this.dataset.resourceId);
            e.dataTransfer.setData('resourceType', this.dataset.resourceType);
            e.dataTransfer.setData('resourceName', this.dataset.resourceName);
        });
    });
}

function initCanvasEvents() {
    const canvasArea = document.getElementById('canvasArea');
    
    canvasArea.addEventListener('dragover', function(e) {
        e.preventDefault();
        this.classList.add('drag-over');
    });

    canvasArea.addEventListener('dragleave', function(e) {
        if (!this.contains(e.relatedTarget)) {
            this.classList.remove('drag-over');
        }
    });

    canvasArea.addEventListener('drop', function(e) {
        e.preventDefault();
        this.classList.remove('drag-over');
        
        const resourceId = e.dataTransfer.getData('resourceId');
        const resourceType = e.dataTransfer.getData('resourceType');
        const resourceName = e.dataTransfer.getData('resourceName');
        
        if (resourceId) {
            const rect = this.getBoundingClientRect();
            const x = e.clientX - rect.left - 100;
            const y = e.clientY - rect.top - 40;
            addCanvasNode(resourceId, resourceType, resourceName, x, y);
        }
    });

    canvasArea.addEventListener('click', function(e) {
        if (e.target === this || e.target.classList.contains('canvas-connections') || e.target.classList.contains('canvas-nodes')) {
            selectNode(null);
            selectConnection(null);
        }
    });

    document.addEventListener('keydown', function(e) {
        if (currentTab !== 'canvas') return;
        if (e.key === 'Delete') {
            if (selectedConnection) {
                deleteConnection(selectedConnection);
            } else if (selectedNode) {
                deleteCanvasNode(selectedNode);
            }
        }
    });
}

function addCanvasNode(resourceId, resourceType, resourceName, x, y) {
    nodeIdCounter++;
    const nodeId = `node_${nodeIdCounter}`;
    const node = {
        id: nodeId,
        resourceId: resourceId,
        type: resourceType,
        name: resourceName,
        x: Math.max(0, x),
        y: Math.max(0, y),
        nodeType: 'source',
        tables: [],
    };
    canvasNodes.push(node);
    renderCanvas();
    selectNode(nodeId);
    updateCanvasEmptyState();
}

function getConnectionEndpoints(conn) {
    const fromNode = canvasNodes.find(n => n.id === conn.from);
    const toNode = canvasNodes.find(n => n.id === conn.to);
    if (!fromNode || !toNode) return null;

    const nodeHeight = 80;
    const nodeWidth = 200;

    const fromOutgoing = canvasConnections.filter(c => c.from === conn.from);
    const toIncoming = canvasConnections.filter(c => c.to === conn.to);
    const fromIdx = fromOutgoing.indexOf(conn);
    const toIdx = toIncoming.indexOf(conn);
    const fromTotal = fromOutgoing.length;
    const toTotal = toIncoming.length;

    const fromSpacing = Math.min(25, nodeHeight / (fromTotal + 1));
    const toSpacing = Math.min(25, nodeHeight / (toTotal + 1));

    const fromY = fromNode.y + (fromIdx + 1) * fromSpacing + 10;
    const toY = toNode.y + (toIdx + 1) * toSpacing + 10;

    return {
        x1: fromNode.x + nodeWidth,
        y1: fromY,
        x2: toNode.x,
        y2: toY
    };
}

const CARDINALITY_COLORS = {
    '1:1': '#6366f1',
    '1:N': '#10b981',
    'N:1': '#f59e0b',
    'N:M': '#ef4444'
};

const CARDINALITY_LABELS = {
    '1:1': '一对一',
    '1:N': '一对多',
    'N:1': '多对一',
    'N:M': '多对多'
};

function renderCanvas() {
    const nodesContainer = document.getElementById('canvasNodes');
    const connectionsContainer = document.getElementById('canvasConnections');

    nodesContainer.innerHTML = canvasNodes.map(node => {
        const outCount = canvasConnections.filter(c => c.from === node.id).length;
        const inCount = canvasConnections.filter(c => c.to === node.id).length;
        return `
        <div class="canvas-node ${selectedNode === node.id ? 'selected' : ''}" 
             data-node-id="${node.id}"
             style="left: ${node.x}px; top: ${node.y}px;">
            <div class="canvas-node-header">
                <div class="canvas-node-icon ${node.type}">
                    <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                        <ellipse cx="12" cy="5" rx="9" ry="3"></ellipse>
                        <path d="M21 12c0 1.66-4 3-9 3s-9-1.34-9-3"></path>
                        <path d="M3 5v14c0 1.66 4 3 9 3s9-1.34 9-3V5"></path>
                    </svg>
                </div>
                <span class="canvas-node-title">${node.name}</span>
                <span class="canvas-node-type-badge">${node.nodeType === 'source' ? '源' : node.nodeType === 'both' ? '中转' : '目标'}</span>
            </div>
            <div class="canvas-node-body">
                <span class="canvas-node-db">${DB_TYPE_LABELS[node.type] || node.type}</span>
                ${node.tables.length > 0 ? `<span class="canvas-node-tables">${node.tables.length} 张表</span>` : ''}
                ${(outCount > 0 || inCount > 0) ? `<span class="canvas-node-conn-count">入${inCount} 出${outCount}</span>` : ''}
            </div>
            <div class="canvas-node-port port-input" data-node-id="${node.id}" data-port-type="input" title="输入端口"></div>
            <div class="canvas-node-port port-output" data-node-id="${node.id}" data-port-type="output" title="输出端口"></div>
        </div>
    `}).join('');

    const svgNS = "http://www.w3.org/2000/svg";
    connectionsContainer.innerHTML = '';

    canvasConnections.forEach(conn => {
        const endpoints = getConnectionEndpoints(conn);
        if (!endpoints) return;

        const { x1, y1, x2, y2 } = endpoints;
        const color = CARDINALITY_COLORS[conn.cardinality] || '#6366f1';
        const isSelected = selectedConnection === conn.id;

        const g = document.createElementNS(svgNS, 'g');
        g.setAttribute('data-connection-id', conn.id);
        g.style.cursor = 'pointer';

        const hitArea = document.createElementNS(svgNS, 'path');
        const dx = x2 - x1;
        const controlOffset = Math.min(Math.abs(dx) * 0.5, 150);
        const midX1 = x1 + controlOffset;
        const midX2 = x2 - controlOffset;
        const d = `M ${x1} ${y1} C ${midX1} ${y1}, ${midX2} ${y2}, ${x2} ${y2}`;
        hitArea.setAttribute('d', d);
        hitArea.setAttribute('stroke', 'transparent');
        hitArea.setAttribute('stroke-width', '16');
        hitArea.setAttribute('fill', 'none');
        hitArea.style.pointerEvents = 'stroke';
        g.appendChild(hitArea);

        const line = document.createElementNS(svgNS, 'path');
        line.setAttribute('d', d);
        line.setAttribute('stroke', color);
        line.setAttribute('stroke-width', isSelected ? '3.5' : '2.5');
        line.setAttribute('fill', 'none');
        line.setAttribute('stroke-linecap', 'round');
        line.style.pointerEvents = 'none';
        line.style.filter = isSelected
            ? `drop-shadow(0 0 6px ${color}80)`
            : `drop-shadow(0 1px 2px ${color}30)`;
        if (isSelected) {
            line.setAttribute('stroke-dasharray', '8 4');
        }
        g.appendChild(line);

        const arrowSize = 8;
        const angle = Math.atan2(y2 - (y2 + y1) / 2, x2 - midX2);
        const arrowX1 = x2 - arrowSize * Math.cos(angle - Math.PI / 6);
        const arrowY1 = y2 - arrowSize * Math.sin(angle - Math.PI / 6);
        const arrowX2 = x2 - arrowSize * Math.cos(angle + Math.PI / 6);
        const arrowY2 = y2 - arrowSize * Math.sin(angle + Math.PI / 6);
        const arrow = document.createElementNS(svgNS, 'polygon');
        arrow.setAttribute('points', `${x2},${y2} ${arrowX1},${arrowY1} ${arrowX2},${arrowY2}`);
        arrow.setAttribute('fill', color);
        arrow.style.pointerEvents = 'none';
        g.appendChild(arrow);

        const cardinality = conn.cardinality || '1:1';
        const parts = cardinality.split(':');

        const srcLabel = document.createElementNS(svgNS, 'text');
        srcLabel.setAttribute('x', x1 + 16);
        srcLabel.setAttribute('y', y1 - 8);
        srcLabel.setAttribute('fill', color);
        srcLabel.setAttribute('font-size', '12');
        srcLabel.setAttribute('font-weight', '700');
        srcLabel.setAttribute('font-family', 'monospace');
        srcLabel.style.pointerEvents = 'none';
        srcLabel.textContent = parts[0];
        g.appendChild(srcLabel);

        const tgtLabel = document.createElementNS(svgNS, 'text');
        tgtLabel.setAttribute('x', x2 - 24);
        tgtLabel.setAttribute('y', y2 - 8);
        tgtLabel.setAttribute('fill', color);
        tgtLabel.setAttribute('font-size', '12');
        tgtLabel.setAttribute('font-weight', '700');
        tgtLabel.setAttribute('font-family', 'monospace');
        tgtLabel.style.pointerEvents = 'none';
        tgtLabel.textContent = parts[1];
        g.appendChild(tgtLabel);

        const midX = (x1 + x2) / 2;
        const midY = (y1 + y2) / 2;
        const typeLabel = document.createElementNS(svgNS, 'text');
        typeLabel.setAttribute('x', midX);
        typeLabel.setAttribute('y', midY - 10);
        typeLabel.setAttribute('fill', color);
        typeLabel.setAttribute('font-size', '11');
        typeLabel.setAttribute('font-weight', '600');
        typeLabel.setAttribute('text-anchor', 'middle');
        typeLabel.setAttribute('font-family', '-apple-system, sans-serif');
        typeLabel.style.pointerEvents = 'none';
        typeLabel.style.background = 'white';
        typeLabel.textContent = CARDINALITY_LABELS[cardinality] || cardinality;
        g.appendChild(typeLabel);

        const bg = document.createElementNS(svgNS, 'rect');
        const textLen = (CARDINALITY_LABELS[cardinality] || cardinality).length * 7 + 12;
        bg.setAttribute('x', midX - textLen / 2);
        bg.setAttribute('y', midY - 22);
        bg.setAttribute('width', textLen);
        bg.setAttribute('height', 18);
        bg.setAttribute('rx', '4');
        bg.setAttribute('fill', 'white');
        bg.setAttribute('fill-opacity', '0.85');
        bg.setAttribute('stroke', color);
        bg.setAttribute('stroke-width', '1');
        bg.style.pointerEvents = 'none';
        g.insertBefore(bg, typeLabel);

        g.addEventListener('click', function(e) {
            e.stopPropagation();
            selectConnection(conn.id);
        });

        connectionsContainer.appendChild(g);
    });

    if (isConnecting && tempConnectionLine) {
        connectionsContainer.appendChild(tempConnectionLine);
    }

    initNodeEvents();
    updateCanvasEmptyState();
}

function initGlobalCanvasEvents() {
    const canvasArea = document.getElementById('canvasArea');

    document.addEventListener('mousemove', function(e) {
        if (currentTab !== 'canvas') return;

        if (isDragging && selectedNode) {
            const rect = canvasArea.getBoundingClientRect();
            const node = canvasNodes.find(n => n.id === selectedNode);
            if (node) {
                node.x = Math.max(0, e.clientX - rect.left - dragOffset.x + canvasArea.scrollLeft);
                node.y = Math.max(0, e.clientY - rect.top - dragOffset.y + canvasArea.scrollTop);
                renderCanvas();
            }
        }

        if (isConnecting && tempConnectionLine && connectingFrom) {
            const rect = canvasArea.getBoundingClientRect();
            const fromNode = canvasNodes.find(n => n.id === connectingFrom.nodeId);
            if (fromNode) {
                const x1 = fromNode.x + 200;
                const y1 = fromNode.y + 40;
                const x2 = e.clientX - rect.left + canvasArea.scrollLeft;
                const y2 = e.clientY - rect.top + canvasArea.scrollTop;
                const dx = x2 - x1;
                const controlOffset = Math.min(Math.abs(dx) * 0.5, 150);
                const midX1 = x1 + controlOffset;
                const midX2 = x2 - controlOffset;
                const d = `M ${x1} ${y1} C ${midX1} ${y1}, ${midX2} ${y2}, ${x2} ${y2}`;
                tempConnectionLine.setAttribute('d', d);
            }
        }
    });

    document.addEventListener('mouseup', function(e) {
        if (currentTab !== 'canvas') return;

        if (isConnecting && connectingFrom) {
            const target = e.target;
            let toNodeId = null;

            if (target.classList.contains('canvas-node-port') && target.dataset.portType === 'input') {
                toNodeId = target.dataset.nodeId;
            } else if (target.classList.contains('canvas-node') || target.closest('.canvas-node')) {
                const nodeEl = target.classList.contains('canvas-node') ? target : target.closest('.canvas-node');
                toNodeId = nodeEl.dataset.nodeId;
            }

            if (!toNodeId) {
                const area = document.getElementById('canvasArea');
                const rect = area.getBoundingClientRect();
                const mouseX = e.clientX - rect.left + area.scrollLeft;
                const mouseY = e.clientY - rect.top + area.scrollTop;

                let minDistance = 50;
                canvasNodes.forEach(node => {
                    if (node.id === connectingFrom.nodeId) return;
                    const centerX = node.x + 100;
                    const centerY = node.y + 40;
                    const distance = Math.sqrt(Math.pow(mouseX - centerX, 2) + Math.pow(mouseY - centerY, 2));
                    if (distance < minDistance) {
                        minDistance = distance;
                        toNodeId = node.id;
                    }
                });
            }

            if (toNodeId && connectingFrom.nodeId !== toNodeId) {
                const exists = canvasConnections.some(c => c.from === connectingFrom.nodeId && c.to === toNodeId);
                if (!exists) {
                    pendingConnection = { from: connectingFrom.nodeId, to: toNodeId };
                    showCardinalityModal(connectingFrom.nodeId, toNodeId);
                }
            }
        }

        isDragging = false;
        cancelConnecting();
    });
}

function initNodeEvents() {
    document.querySelectorAll('.canvas-node').forEach(node => {
        if (node.dataset.eventsBound) return;
        node.dataset.eventsBound = 'true';
        
        const nodeId = node.dataset.nodeId;

        node.addEventListener('mousedown', function(e) {
            if (e.target.classList.contains('canvas-node-port')) return;
            
            isDragging = true;
            selectedNode = nodeId;
            const nodeData = canvasNodes.find(n => n.id === nodeId);
            if (nodeData) {
                dragOffset.x = e.clientX - nodeData.x;
                dragOffset.y = e.clientY - nodeData.y;
            }
            renderCanvas();
        });

        node.addEventListener('dblclick', function(e) {
            if (!e.target.classList.contains('canvas-node-port')) {
                openNodeConfig(nodeId);
            }
        });
    });

    document.querySelectorAll('.canvas-node-port').forEach(port => {
        if (port.dataset.eventsBound) return;
        port.dataset.eventsBound = 'true';
        
        port.addEventListener('mousedown', function(e) {
            e.stopPropagation();
            const nodeId = this.dataset.nodeId;
            const portType = this.dataset.portType;
            
            if (portType === 'output') {
                isConnecting = true;
                connectingFrom = { nodeId, portType };
                
                const svgNS = "http://www.w3.org/2000/svg";
                tempConnectionLine = document.createElementNS(svgNS, 'path');
                tempConnectionLine.setAttribute('class', 'temp');
                tempConnectionLine.setAttribute('fill', 'none');
                tempConnectionLine.setAttribute('stroke', '#818cf8');
                tempConnectionLine.setAttribute('stroke-width', '3');
                tempConnectionLine.setAttribute('stroke-dasharray', '8 4');
                tempConnectionLine.setAttribute('stroke-linecap', 'round');
                document.getElementById('canvasConnections').appendChild(tempConnectionLine);
            }
        });
    });
}

function cancelConnecting() {
    isConnecting = false;
    connectingFrom = null;
    if (tempConnectionLine) {
        tempConnectionLine.remove();
        tempConnectionLine = null;
    }
}

function addConnection(fromNodeId, toNodeId, cardinality) {
    const exists = canvasConnections.some(c => c.from === fromNodeId && c.to === toNodeId);
    if (exists) return;

    const connectionId = `conn_${Date.now()}`;
    canvasConnections.push({
        id: connectionId,
        from: fromNodeId,
        to: toNodeId,
        cardinality: cardinality || '1:1',
        tableMappings: [],
    });

    _refreshNodeTypes();
    
    renderCanvas();
    showToast(`${CARDINALITY_LABELS[cardinality || '1:1']}连接已建立`, 'success');

    if (cardinality && cardinality !== '1:1') {
        openTableMappingModal(connectionId);
    }
}

function _refreshNodeTypes() {
    canvasNodes.forEach(n => { n.nodeType = 'source'; });
    canvasConnections.forEach(c => {
        const toNode = canvasNodes.find(n => n.id === c.to);
        if (toNode) toNode.nodeType = 'target';
    });
    canvasNodes.forEach(n => {
        const hasOutgoing = canvasConnections.some(c => c.from === n.id);
        const hasIncoming = canvasConnections.some(c => c.to === n.id);
        if (hasOutgoing && hasIncoming) n.nodeType = 'both';
    });
}

function selectConnection(connId) {
    selectedConnection = connId;
    if (connId) selectedNode = null;
    renderCanvas();
    renderInspector();
}

function deleteConnection(connId) {
    canvasConnections = canvasConnections.filter(c => c.id !== connId);
    selectedConnection = null;
    _refreshNodeTypes();
    renderCanvas();
    renderInspector();
    showToast('连接已删除', 'info');
}

function updateConnectionCardinality(connId, newCardinality) {
    const conn = canvasConnections.find(c => c.id === connId);
    if (conn) {
        const oldCard = conn.cardinality;
        conn.cardinality = newCardinality;
        if (oldCard !== newCardinality) {
            conn.tableMappings = [];
        }
        renderCanvas();
        renderInspector();
        showToast(`关联类型已更新为${CARDINALITY_LABELS[newCardinality]}`, 'success');
        if (newCardinality !== '1:1' && oldCard !== newCardinality) {
            setTimeout(() => openTableMappingModal(connId), 300);
        }
    }
}

function showCardinalityModal(fromNodeId, toNodeId) {
    const fromNode = canvasNodes.find(n => n.id === fromNodeId);
    const toNode = canvasNodes.find(n => n.id === toNodeId);
    if (!fromNode || !toNode) return;

    document.getElementById('cardinalityFromName').textContent = fromNode.name;
    document.getElementById('cardinalityToName').textContent = toNode.name;

    const defaultRadio = document.querySelector('input[name="cardinality"][value="1:1"]');
    if (defaultRadio) defaultRadio.checked = true;

    document.getElementById('cardinalityModal').classList.add('active');
}

function closeCardinalityModal() {
    document.getElementById('cardinalityModal').classList.remove('active');
    pendingConnection = null;
}

function confirmCardinality() {
    if (!pendingConnection) return;

    const selected = document.querySelector('input[name="cardinality"]:checked');
    const cardinality = selected ? selected.value : '1:1';

    addConnection(pendingConnection.from, pendingConnection.to, cardinality);
    closeCardinalityModal();
}

function selectNode(nodeId) {
    selectedNode = nodeId;
    if (nodeId) selectedConnection = null;
    renderCanvas();
    renderInspector();
}

function deleteCanvasNode(nodeId) {
    canvasNodes = canvasNodes.filter(n => n.id !== nodeId);
    canvasConnections = canvasConnections.filter(c => c.from !== nodeId && c.to !== nodeId);
    if (selectedNode === nodeId) selectedNode = null;
    selectedConnection = null;
    renderCanvas();
    renderInspector();
    showToast('节点已删除', 'info');
}

function updateCanvasEmptyState() {
    const emptyEl = document.getElementById('canvasEmpty');
    if (canvasNodes.length === 0) {
        emptyEl.style.display = 'flex';
    } else {
        emptyEl.style.display = 'none';
    }
}

function clearCanvas() {
    if (canvasNodes.length === 0) return;
    if (!confirm('确定要清空画布吗？所有节点和连接都将被清除。')) return;
    
    canvasNodes = [];
    canvasConnections = [];
    selectedNode = null;
    selectedConnection = null;
    renderCanvas();
    renderInspector();
    showToast('画布已清空', 'info');
}

function autoLayout() {
    if (canvasNodes.length === 0) return;
    
    const sourceNodes = canvasNodes.filter(n => n.nodeType === 'source' || canvasConnections.some(c => c.from === n.id));
    const targetNodes = canvasNodes.filter(n => n.nodeType === 'target' || canvasConnections.some(c => c.to === n.id));
    const otherNodes = canvasNodes.filter(n => !sourceNodes.includes(n) && !targetNodes.includes(n));
    
    const startX = 100;
    const startY = 80;
    const spacingY = 120;
    
    sourceNodes.forEach((node, index) => {
        node.x = startX;
        node.y = startY + index * spacingY;
    });
    
    targetNodes.forEach((node, index) => {
        node.x = startX + 450;
        node.y = startY + index * spacingY;
    });
    
    otherNodes.forEach((node, index) => {
        node.x = startX + 225;
        node.y = startY + (sourceNodes.length + index) * spacingY;
    });
    
    renderCanvas();
    showToast('自动布局完成', 'success');
}

function renderInspector() {
    const body = document.getElementById('inspectorBody');
    
    if (selectedConnection) {
        renderConnectionInspector(body);
        return;
    }

    if (!selectedNode) {
        body.innerHTML = `
            <div class="inspector-empty">
                <svg width="40" height="40" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5">
                    <circle cx="12" cy="12" r="3"></circle>
                    <path d="M19.4 15a1.65 1.65 0 0 0 .33 1.82l.06.06a2 2 0 0 1 0 2.83 2 2 0 0 1-2.83 0l-.06-.06a1.65 1.65 0 0 0-1.82-.33 1.65 1.65 0 0 0-1 1.51V21a2 2 0 0 1-2 2 2 2 0 0 1-2-2v-.09A1.65 1.65 0 0 0 9 19.4a1.65 1.65 0 0 0-1.82.33l-.06.06a2 2 0 0 1-2.83 0 2 2 0 0 1 0-2.83l.06-.06a1.65 1.65 0 0 0 .33-1.82 1.65 1.65 0 0 0-1.51-1H3a2 2 0 0 1-2-2 2 2 0 0 1 2-2h.09A1.65 1.65 0 0 0 4.6 9a1.65 1.65 0 0 0-.33-1.82l-.06-.06a2 2 0 0 1 0-2.83 2 2 0 0 1 2.83 0l.06.06a1.65 1.65 0 0 0 1.82.33H9a1.65 1.65 0 0 0 1-1.51V3a2 2 0 0 1 2-2 2 2 0 0 1 2 2v.09a1.65 1.65 0 0 0 1 1.51 1.65 1.65 0 0 0 1.82-.33l.06-.06a2 2 0 0 1 2.83 0 2 2 0 0 1 0 2.83l-.06.06a1.65 1.65 0 0 0-.33 1.82V9a1.65 1.65 0 0 0 1.51 1H21a2 2 0 0 1 2 2 2 2 0 0 1-2 2h-.09a1.65 1.65 0 0 0-1.51 1z"></path>
                </svg>
                <p>选中节点或连线查看属性</p>
            </div>
        `;
        return;
    }

    const node = canvasNodes.find(n => n.id === selectedNode);
    if (!node) return;

    const nodeConns = canvasConnections.filter(c => c.from === node.id || c.to === node.id);

    body.innerHTML = `
        <div class="inspector-section">
            <h5>基本信息</h5>
            <div class="form-group">
                <label>节点名称</label>
                <input type="text" value="${node.name}" onchange="updateNodeProperty('${node.id}', 'name', this.value)">
            </div>
            <div class="form-group">
                <label>节点类型</label>
                <select onchange="updateNodeProperty('${node.id}', 'nodeType', this.value)">
                    <option value="source" ${node.nodeType === 'source' ? 'selected' : ''}>源节点</option>
                    <option value="target" ${node.nodeType === 'target' ? 'selected' : ''}>目标节点</option>
                </select>
            </div>
            <div class="form-group">
                <label>数据库类型</label>
                <input type="text" value="${DB_TYPE_LABELS[node.type] || node.type}" disabled>
            </div>
        </div>
        <div class="inspector-section">
            <h5>关联关系 (${nodeConns.length})</h5>
            ${nodeConns.length > 0 ? `
                <div class="inspector-connections-list">
                    ${nodeConns.map(conn => {
                        const isFrom = conn.from === node.id;
                        const otherNode = canvasNodes.find(n => n.id === (isFrom ? conn.to : conn.from));
                        const card = conn.cardinality || '1:1';
                        const color = CARDINALITY_COLORS[card] || '#6366f1';
                        return `
                            <div class="inspector-conn-item" onclick="selectConnection('${conn.id}')" style="border-left: 3px solid ${color};">
                                <div class="inspector-conn-info">
                                    <span class="inspector-conn-direction">${isFrom ? '→' : '←'}</span>
                                    <span class="inspector-conn-name">${otherNode ? otherNode.name : '未知'}</span>
                                </div>
                                <span class="inspector-conn-badge" style="background: ${color}20; color: ${color};">${card}</span>
                            </div>
                        `;
                    }).join('')}
                </div>
            ` : '<p style="font-size: 12px; color: var(--text-muted);">暂无关联关系</p>'}
        </div>
        <div class="inspector-section">
            <h5>同步配置</h5>
            <button class="btn-secondary btn-sm btn-block" onclick="loadNodeTables('${node.id}')">
                ${node.tables.length > 0 ? `已选择 ${node.tables.length} 张表` : '选择同步表'}
            </button>
            ${node.tables.length > 0 ? `
                <div class="selected-tables-list">
                    ${node.tables.map(t => `<span class="table-tag">${t}</span>`).join('')}
                </div>
            ` : ''}
        </div>
        <div class="inspector-section">
            <h5>操作</h5>
            <button class="btn-secondary btn-sm btn-block" onclick="openNodeConfig('${node.id}')">详细配置</button>
            <button class="btn-danger btn-sm btn-block" onclick="deleteCanvasNode('${node.id}')">删除节点</button>
        </div>
    `;
}

function renderConnectionInspector(body) {
    const conn = canvasConnections.find(c => c.id === selectedConnection);
    if (!conn) {
        body.innerHTML = `<div class="inspector-empty"><p>连线不存在</p></div>`;
        return;
    }

    const fromNode = canvasNodes.find(n => n.id === conn.from);
    const toNode = canvasNodes.find(n => n.id === conn.to);
    const card = conn.cardinality || '1:1';
    const color = CARDINALITY_COLORS[card] || '#6366f1';
    const label = CARDINALITY_LABELS[card] || card;

    const tableMappings = conn.tableMappings || [];
    const mappingCount = tableMappings.length;

    body.innerHTML = `
        <div class="inspector-section">
            <h5 style="display: flex; align-items: center; gap: 8px;">
                <span style="width: 10px; height: 10px; border-radius: 50%; background: ${color};"></span>
                关联关系
            </h5>
            <div class="inspector-conn-detail">
                <div class="inspector-conn-flow">
                    <div class="inspector-conn-node">
                        <span class="inspector-conn-node-name">${fromNode ? fromNode.name : '未知'}</span>
                        <span class="inspector-conn-node-type">${fromNode ? (fromNode.nodeType === 'source' ? '源' : fromNode.nodeType === 'both' ? '中转' : '目标') : ''}</span>
                    </div>
                    <div class="inspector-conn-arrow" style="color: ${color};">
                        <svg width="32" height="16" viewBox="0 0 32 16" fill="none" stroke="currentColor" stroke-width="2"><line x1="0" y1="8" x2="24" y2="8"></line><polyline points="20 3 26 8 20 13"></polyline></svg>
                        <span style="font-size: 11px; font-weight: 700; color: ${color};">${card}</span>
                    </div>
                    <div class="inspector-conn-node">
                        <span class="inspector-conn-node-name">${toNode ? toNode.name : '未知'}</span>
                        <span class="inspector-conn-node-type">${toNode ? (toNode.nodeType === 'source' ? '源' : toNode.nodeType === 'both' ? '中转' : '目标') : ''}</span>
                    </div>
                </div>
            </div>
        </div>
        <div class="inspector-section">
            <h5>关联类型</h5>
            <div class="inspector-cardinality-options">
                ${['1:1', '1:N', 'N:1', 'N:M'].map(c => {
                    const cColor = CARDINALITY_COLORS[c];
                    const cLabel = CARDINALITY_LABELS[c];
                    const isActive = card === c;
                    return `
                        <button class="inspector-cardinality-btn ${isActive ? 'active' : ''}"
                                style="border-color: ${isActive ? cColor : 'var(--border-color)'}; background: ${isActive ? cColor + '15' : 'transparent'};"
                                onclick="updateConnectionCardinality('${conn.id}', '${c}')">
                            <span style="color: ${cColor}; font-weight: 700; font-size: 13px;">${c}</span>
                            <span style="font-size: 11px;">${cLabel}</span>
                        </button>
                    `;
                }).join('')}
            </div>
        </div>
        <div class="inspector-section">
            <h5>表映射 (${mappingCount})</h5>
            ${mappingCount > 0 ? `
                <div class="inspector-mappings-list">
                    ${tableMappings.map((tm, idx) => {
                        const srcT = tm.sourceTable || '?';
                        const tgtT = tm.targetTable || '?';
                        const fCnt = (tm.fieldMappings || []).length;
                        return `
                            <div class="inspector-mapping-item" style="border-left: 3px solid ${color};">
                                <span class="mapping-src">${srcT}</span>
                                <span class="mapping-arrow">→</span>
                                <span class="mapping-tgt">${tgtT}</span>
                                <span class="mapping-fields">${fCnt}字段</span>
                            </div>
                        `;
                    }).join('')}
                </div>
            ` : '<p style="font-size: 12px; color: var(--text-muted);">暂未配置表映射</p>'}
            <button class="btn-secondary btn-sm btn-block" style="margin-top: 8px;" onclick="openTableMappingModal('${conn.id}')">
                ${mappingCount > 0 ? '编辑表映射' : '配置表映射'}
            </button>
        </div>
        <div class="inspector-section">
            <h5>操作</h5>
            <button class="btn-danger btn-sm btn-block" onclick="deleteConnection('${conn.id}')">删除连接</button>
        </div>
    `;
}

function updateNodeProperty(nodeId, property, value) {
    const node = canvasNodes.find(n => n.id === nodeId);
    if (node) {
        node[property] = value;
        renderCanvas();
    }
}

async function loadNodeTables(nodeId) {
    const node = canvasNodes.find(n => n.id === nodeId);
    if (!node) return;

    showToast('正在加载表列表...', 'info');
    const data = await apiRequest(`/api/resources/${node.resourceId}/tables`);
    if (data && data.success) {
        const tables = data.tables || [];
        showNodeTableSelector(nodeId, tables);
    } else {
        showToast('加载表列表失败', 'error');
    }
}

function showNodeTableSelector(nodeId, tables) {
    const node = canvasNodes.find(n => n.id === nodeId);
    if (!node) return;

    const selected = node.tables || [];
    
    const modal = document.getElementById('nodeConfigModal');
    const body = document.getElementById('nodeConfigBody');
    document.getElementById('nodeConfigTitle').textContent = `选择同步表 - ${node.name}`;
    
    body.innerHTML = `
        <div style="margin-bottom: 12px;">
            <label class="table-checkbox-item">
                <input type="checkbox" id="selectAllNodeTables" onchange="toggleAllNodeTables(this)" ${selected.length === tables.length ? 'checked' : ''}>
                <strong>全选 (${tables.length} 张表)</strong>
            </label>
        </div>
        <div class="tables-checkboxes" id="nodeTablesCheckboxes">
            ${tables.map(table => `
                <label class="table-checkbox-item">
                    <input type="checkbox" value="${table}" data-node-id="${nodeId}" onchange="toggleNodeTable(this)" ${selected.includes(table) ? 'checked' : ''}>
                    ${table}
                </label>
            `).join('')}
        </div>
    `;
    
    currentConfigNodeId = nodeId;
    modal.classList.add('active');
}

let currentConfigNodeId = null;

function toggleAllNodeTables(checkbox) {
    const checkboxes = document.querySelectorAll('#nodeTablesCheckboxes input[type="checkbox"]');
    checkboxes.forEach(cb => {
        cb.checked = checkbox.checked;
        toggleNodeTable(cb);
    });
}

function toggleNodeTable(checkbox) {
    const nodeId = checkbox.dataset.nodeId || currentConfigNodeId;
    const node = canvasNodes.find(n => n.id === nodeId);
    if (!node) return;

    const table = checkbox.value;
    if (!node.tables) node.tables = [];
    
    if (checkbox.checked) {
        if (!node.tables.includes(table)) {
            node.tables.push(table);
        }
    } else {
        node.tables = node.tables.filter(t => t !== table);
    }
    
    renderInspector();
}

function openNodeConfig(nodeId) {
    const node = canvasNodes.find(n => n.id === nodeId);
    if (!node) return;

    const modal = document.getElementById('nodeConfigModal');
    const body = document.getElementById('nodeConfigBody');
    document.getElementById('nodeConfigTitle').textContent = `节点配置 - ${node.name}`;
    
    body.innerHTML = `
        <div class="form-group">
            <label>节点名称</label>
            <input type="text" id="configNodeName" value="${node.name}">
        </div>
        <div class="form-group">
            <label>节点角色</label>
            <select id="configNodeType">
                <option value="source" ${node.nodeType === 'source' ? 'selected' : ''}>源数据库（读取数据）</option>
                <option value="target" ${node.nodeType === 'target' ? 'selected' : ''}>目标数据库（写入数据）</option>
                <option value="both" ${node.nodeType === 'both' ? 'selected' : ''}>中转（既是源又是目标）</option>
            </select>
        </div>
        <div class="form-group">
            <label>数据库类型</label>
            <input type="text" value="${DB_TYPE_LABELS[node.type] || node.type}" disabled>
        </div>
        <div class="form-group">
            <label>已选表数</label>
            <input type="text" value="${node.tables.length} 张" disabled>
        </div>
        <div style="margin-top: 16px;">
            <button class="btn-secondary btn-sm" onclick="loadNodeTables('${nodeId}')">选择同步表</button>
        </div>
    `;
    
    currentConfigNodeId = nodeId;
    modal.classList.add('active');
}

function saveNodeConfig() {
    if (!currentConfigNodeId) return;
    
    const node = canvasNodes.find(n => n.id === currentConfigNodeId);
    if (!node) return;
    
    const nameInput = document.getElementById('configNodeName');
    const typeSelect = document.getElementById('configNodeType');
    
    if (nameInput) node.name = nameInput.value;
    if (typeSelect) node.nodeType = typeSelect.value;
    
    closeNodeConfigModal();
    renderCanvas();
    renderInspector();
    showToast('节点配置已保存', 'success');
}

function closeNodeConfigModal() {
    document.getElementById('nodeConfigModal').classList.remove('active');
    currentConfigNodeId = null;
}

let currentMappingConnId = null;
let currentFieldMappingIdx = null;
let tempTableMappings = [];
let sourceTableList = [];
let targetTableList = [];

async function openTableMappingModal(connId) {
    const conn = canvasConnections.find(c => c.id === connId);
    if (!conn) return;

    currentMappingConnId = connId;
    const fromNode = canvasNodes.find(n => n.id === conn.from);
    const toNode = canvasNodes.find(n => n.id === conn.to);
    const card = conn.cardinality || '1:1';
    const color = CARDINALITY_COLORS[card] || '#6366f1';

    document.getElementById('tableMappingTitle').textContent =
        `表映射编辑器 - ${fromNode ? fromNode.name : '?'} → ${toNode ? toNode.name : '?'} (${CARDINALITY_LABELS[card]})`;

    tempTableMappings = JSON.parse(JSON.stringify(conn.tableMappings || []));

    showToast('正在加载表列表...', 'info');
    const [srcData, tgtData] = await Promise.all([
        apiRequest(`/api/resources/${fromNode.resourceId}/tables`),
        apiRequest(`/api/resources/${toNode.resourceId}/tables`)
    ]);

    sourceTableList = (srcData && srcData.success) ? srcData.tables : [];
    targetTableList = (tgtData && tgtData.success) ? tgtData.tables : [];

    renderTableMappingBody(card, color);
    document.getElementById('tableMappingModal').classList.add('active');
}

function renderTableMappingBody(card, color) {
    const body = document.getElementById('tableMappingBody');
    const cardLabel = CARDINALITY_LABELS[card] || card;

    let addBtnHtml = '';
    if (card === '1:N') {
        addBtnHtml = `<button class="btn-secondary btn-sm" onclick="add1toNMapping()">+ 添加拆分映射（1源表→1目标表）</button>`;
    } else if (card === 'N:1') {
        addBtnHtml = `<button class="btn-secondary btn-sm" onclick="addNto1Mapping()">+ 添加合并映射（1源表→同1目标表）</button>`;
    } else if (card === 'N:M') {
        addBtnHtml = `<button class="btn-secondary btn-sm" onclick="addNto_MMapping()">+ 添加交叉映射</button>`;
    } else {
        addBtnHtml = `<button class="btn-secondary btn-sm" onclick="add1to1Mapping()">+ 添加表映射</button>`;
    }

    body.innerHTML = `
        <div style="margin-bottom: 12px; display: flex; align-items: center; gap: 8px;">
            <span style="background: ${color}20; color: ${color}; padding: 2px 10px; border-radius: 4px; font-weight: 600; font-size: 12px;">${card} ${cardLabel}</span>
            <span style="font-size: 12px; color: var(--text-muted);">配置源表到目标表的映射关系</span>
        </div>
        <div class="table-mapping-list" id="tableMappingList">
            ${tempTableMappings.length === 0 ? `
                <div class="mapping-empty">
                    <p>暂未配置表映射，请点击下方按钮添加</p>
                </div>
            ` : tempTableMappings.map((tm, idx) => {
                const srcT = tm.sourceTable || '';
                const tgtT = tm.targetTable || '';
                const fCnt = (tm.fieldMappings || []).length;
                return `
                    <div class="table-mapping-row">
                        <div class="mapping-row-fields">
                            <select class="mapping-select" onchange="updateTempMapping(${idx}, 'sourceTable', this.value)">
                                <option value="">选择源表</option>
                                ${sourceTableList.map(t => `<option value="${t}" ${t === srcT ? 'selected' : ''}>${t}</option>`).join('')}
                            </select>
                            <span class="mapping-row-arrow">→</span>
                            <select class="mapping-select" onchange="updateTempMapping(${idx}, 'targetTable', this.value)"
                                    ${card === 'N:1' ? '' : ''}>
                                <option value="">选择目标表</option>
                                ${targetTableList.map(t => `<option value="${t}" ${t === tgtT ? 'selected' : ''}>${t}</option>`).join('')}
                            </select>
                            <button class="btn-secondary btn-xs" onclick="openFieldMappingModal(${idx})" title="编辑字段映射">
                                ${fCnt > 0 ? `${fCnt}字段` : '字段映射'}
                            </button>
                            <button class="btn-danger btn-xs" onclick="removeTempMapping(${idx})" title="删除">✕</button>
                        </div>
                    </div>
                `;
            }).join('')}
        </div>
        <div style="margin-top: 12px;">
            ${addBtnHtml}
        </div>
    `;
}

function add1to1Mapping() {
    tempTableMappings.push({ sourceTable: '', targetTable: '', fieldMappings: [] });
    const conn = canvasConnections.find(c => c.id === currentMappingConnId);
    renderTableMappingBody(conn ? (conn.cardinality || '1:1') : '1:1',
        CARDINALITY_COLORS[conn ? (conn.cardinality || '1:1') : '1:1'] || '#6366f1');
}

function add1toNMapping() {
    tempTableMappings.push({ sourceTable: '', targetTable: '', fieldMappings: [] });
    const conn = canvasConnections.find(c => c.id === currentMappingConnId);
    renderTableMappingBody(conn ? (conn.cardinality || '1:N') : '1:N',
        CARDINALITY_COLORS[conn ? (conn.cardinality || '1:N') : '1:N'] || '#10b981');
}

function addNto1Mapping() {
    const existingTarget = tempTableMappings.length > 0 ? tempTableMappings[0].targetTable : '';
    tempTableMappings.push({ sourceTable: '', targetTable: existingTarget, fieldMappings: [] });
    const conn = canvasConnections.find(c => c.id === currentMappingConnId);
    renderTableMappingBody(conn ? (conn.cardinality || 'N:1') : 'N:1',
        CARDINALITY_COLORS[conn ? (conn.cardinality || 'N:1') : 'N:1'] || '#f59e0b');
}

function addNto_MMapping() {
    tempTableMappings.push({ sourceTable: '', targetTable: '', fieldMappings: [] });
    const conn = canvasConnections.find(c => c.id === currentMappingConnId);
    renderTableMappingBody(conn ? (conn.cardinality || 'N:M') : 'N:M',
        CARDINALITY_COLORS[conn ? (conn.cardinality || 'N:M') : 'N:M'] || '#ef4444');
}

function updateTempMapping(idx, field, value) {
    if (idx >= 0 && idx < tempTableMappings.length) {
        tempTableMappings[idx][field] = value;
        const conn = canvasConnections.find(c => c.id === currentMappingConnId);
        if (conn && conn.cardinality === 'N:1' && field === 'targetTable') {
            tempTableMappings.forEach(tm => { tm.targetTable = value; });
        }
        renderTableMappingBody(conn.cardinality || '1:1',
            CARDINALITY_COLORS[conn.cardinality || '1:1'] || '#6366f1');
    }
}

function removeTempMapping(idx) {
    tempTableMappings.splice(idx, 1);
    const conn = canvasConnections.find(c => c.id === currentMappingConnId);
    renderTableMappingBody(conn.cardinality || '1:1',
        CARDINALITY_COLORS[conn.cardinality || '1:1'] || '#6366f1');
}

function saveTableMappings() {
    const conn = canvasConnections.find(c => c.id === currentMappingConnId);
    if (!conn) return;

    const validMappings = tempTableMappings.filter(tm => tm.sourceTable);
    conn.tableMappings = validMappings;
    closeTableMappingModal();
    renderCanvas();
    renderInspector();
    showToast(`表映射已保存 (${validMappings.length} 条)`, 'success');
}

function closeTableMappingModal() {
    document.getElementById('tableMappingModal').classList.remove('active');
    currentMappingConnId = null;
}

let tempFieldMappings = [];
let sourceColumnList = [];
let targetColumnList = [];

async function openFieldMappingModal(mappingIdx) {
    if (mappingIdx < 0 || mappingIdx >= tempTableMappings.length) return;

    currentFieldMappingIdx = mappingIdx;
    const tm = tempTableMappings[mappingIdx];

    if (!tm.sourceTable || !tm.targetTable) {
        showToast('请先选择源表和目标表', 'error');
        return;
    }

    const conn = canvasConnections.find(c => c.id === currentMappingConnId);
    const fromNode = canvasNodes.find(n => n.id === conn.from);
    const toNode = canvasNodes.find(n => n.id === conn.to);

    document.getElementById('fieldMappingTitle').textContent =
        `字段映射: ${tm.sourceTable} → ${tm.targetTable}`;

    tempFieldMappings = JSON.parse(JSON.stringify(tm.fieldMappings || []));

    showToast('正在加载字段列表...', 'info');
    const [srcColData, tgtColData] = await Promise.all([
        apiRequest(`/api/resources/${fromNode.resourceId}/tables/${tm.sourceTable}/columns`),
        apiRequest(`/api/resources/${toNode.resourceId}/tables/${tm.targetTable}/columns`)
    ]);

    sourceColumnList = (srcColData && srcColData.success) ? srcColData.columns : [];
    targetColumnList = (tgtColData && tgtColData.success) ? tgtColData.columns : [];

    if (tempFieldMappings.length === 0 && sourceColumnList.length > 0) {
        sourceColumnList.forEach(sc => {
            const matchTarget = targetColumnList.find(tc => tc.name.toLowerCase() === sc.name.toLowerCase());
            if (matchTarget) {
                tempFieldMappings.push({ source: sc.name, target: matchTarget.name });
            }
        });
    }

    renderFieldMappingBody();
    document.getElementById('fieldMappingModal').classList.add('active');
}

function renderFieldMappingBody() {
    const body = document.getElementById('fieldMappingBody');

    body.innerHTML = `
        <div style="display: grid; grid-template-columns: 1fr 40px 1fr; gap: 4px; align-items: center; margin-bottom: 8px; font-weight: 600; font-size: 12px; color: var(--text-secondary);">
            <span>源字段</span>
            <span></span>
            <span>目标字段</span>
        </div>
        <div class="field-mapping-list" id="fieldMappingList">
            ${tempFieldMappings.map((fm, idx) => `
                <div class="field-mapping-row">
                    <select class="mapping-select" onchange="updateTempFieldMapping(${idx}, 'source', this.value)">
                        <option value="">选择源字段</option>
                        ${sourceColumnList.map(c => `<option value="${c.name}" ${c.name === fm.source ? 'selected' : ''}>${c.name} (${c.data_type})</option>`).join('')}
                    </select>
                    <span class="mapping-row-arrow">→</span>
                    <select class="mapping-select" onchange="updateTempFieldMapping(${idx}, 'target', this.value)">
                        <option value="">选择目标字段</option>
                        ${targetColumnList.map(c => `<option value="${c.name}" ${c.name === fm.target ? 'selected' : ''}>${c.name} (${c.data_type})</option>`).join('')}
                    </select>
                    <button class="btn-danger btn-xs" onclick="removeTempFieldMapping(${idx})" title="删除">✕</button>
                </div>
            `).join('')}
        </div>
        <div style="margin-top: 12px; display: flex; gap: 8px;">
            <button class="btn-secondary btn-sm" onclick="addFieldMapping()">+ 添加字段映射</button>
            <button class="btn-secondary btn-sm" onclick="autoMatchFields()">自动匹配</button>
        </div>
        <div style="margin-top: 12px; padding: 8px; background: var(--bg-secondary); border-radius: 6px; font-size: 11px; color: var(--text-muted);">
            源表: ${tempTableMappings[currentFieldMappingIdx]?.sourceTable || '?'} (${sourceColumnList.length} 字段) → 
            目标表: ${tempTableMappings[currentFieldMappingIdx]?.targetTable || '?'} (${targetColumnList.length} 字段)
        </div>
    `;
}

function addFieldMapping() {
    tempFieldMappings.push({ source: '', target: '' });
    renderFieldMappingBody();
}

function autoMatchFields() {
    tempFieldMappings = [];
    sourceColumnList.forEach(sc => {
        const matchTarget = targetColumnList.find(tc => tc.name.toLowerCase() === sc.name.toLowerCase());
        if (matchTarget) {
            tempFieldMappings.push({ source: sc.name, target: matchTarget.name });
        }
    });
    renderFieldMappingBody();
    showToast(`自动匹配了 ${tempFieldMappings.length} 个字段`, 'success');
}

function updateTempFieldMapping(idx, field, value) {
    if (idx >= 0 && idx < tempFieldMappings.length) {
        tempFieldMappings[idx][field] = value;
    }
}

function removeTempFieldMapping(idx) {
    tempFieldMappings.splice(idx, 1);
    renderFieldMappingBody();
}

function saveFieldMappings() {
    if (currentFieldMappingIdx === null) return;

    const validMappings = tempFieldMappings.filter(fm => fm.source && fm.target);
    tempTableMappings[currentFieldMappingIdx].fieldMappings = validMappings;
    closeFieldMappingModal();

    const conn = canvasConnections.find(c => c.id === currentMappingConnId);
    if (conn) {
        renderTableMappingBody(conn.cardinality || '1:1',
            CARDINALITY_COLORS[conn.cardinality || '1:1'] || '#6366f1');
    }
    showToast(`字段映射已保存 (${validMappings.length} 条)`, 'success');
}

function closeFieldMappingModal() {
    document.getElementById('fieldMappingModal').classList.remove('active');
    currentFieldMappingIdx = null;
}

async function savePipeline() {
    if (canvasNodes.length === 0) {
        showToast('请先添加节点到画布', 'error');
        return;
    }

    const name = prompt('请输入流水线名称：', `流水线_${new Date().toLocaleDateString()}`);
    if (!name) return;

    const pipeline = {
        name: name,
        nodes: canvasNodes,
        connections: canvasConnections,
        config: {
            parallel_tables: 4,
            chunk_size: 50000,
            batch_insert_size: 5000,
        },
    };

    const data = await apiRequest('/api/pipelines', 'POST', pipeline);
    if (data && data.success) {
        showToast('流水线保存成功', 'success');
    } else {
        showToast(data?.message || '保存失败', 'error');
    }
}

// ====================================================================== //
//                           流水线管理功能
// ====================================================================== //

async function loadPipelines() {
    const data = await apiRequest('/api/pipelines');
    if (!data || !data.success) return;

    const pipelines = data.pipelines || [];
    const container = document.getElementById('pipelineList');

    if (pipelines.length === 0) {
        container.innerHTML = `
            <div class="empty-state">
                <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                    <line x1="18" y1="20" x2="18" y2="10"></line>
                    <line x1="12" y1="20" x2="12" y2="4"></line>
                    <line x1="6" y1="20" x2="6" y2="14"></line>
                </svg>
                <h4>暂无流水线</h4>
                <p>在同步画布中创建您的第一个流水线</p>
                <button class="btn-primary" onclick="switchTab('canvas')">打开画布</button>
            </div>
        `;
        return;
    }

    container.innerHTML = pipelines.map(pipeline => `
        <div class="pipeline-card">
            <div class="pipeline-card-header">
                <div class="pipeline-icon">
                    <svg width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                        <line x1="18" y1="20" x2="18" y2="10"></line>
                        <line x1="12" y1="20" x2="12" y2="4"></line>
                        <line x1="6" y1="20" x2="6" y2="14"></line>
                    </svg>
                </div>
                <div class="pipeline-info">
                    <h4>${pipeline.name}</h4>
                    <span class="pipeline-meta">${pipeline.nodes?.length || 0} 个节点 · ${pipeline.connections?.length || 0} 个关联</span>
                </div>
                <span class="status-badge status-${pipeline.status || 'idle'}">${getStatusLabel(pipeline.status)}</span>
            </div>
            <div class="pipeline-card-body">
                <div class="pipeline-detail-item">
                    <span class="label">创建时间</span>
                    <span class="value">${formatDate(pipeline.created_at)}</span>
                </div>
                ${pipeline.description ? `
                    <div class="pipeline-detail-item">
                        <span class="label">描述</span>
                        <span class="value">${pipeline.description}</span>
                    </div>
                ` : ''}
            </div>
            <div class="pipeline-card-footer">
                <button class="btn-secondary btn-sm" onclick="editPipelineInCanvas('${pipeline.pipeline_id}')">编辑</button>
                <button class="btn-primary btn-sm" onclick="startPipeline('${pipeline.pipeline_id}')">启动同步</button>
                <button class="btn-danger btn-sm" onclick="deletePipeline('${pipeline.pipeline_id}')">删除</button>
            </div>
        </div>
    `).join('');
}

function getStatusLabel(status) {
    const labels = {
        idle: '未运行',
        running: '运行中',
        completed: '已完成',
        failed: '失败',
    };
    return labels[status] || status;
}

async function editPipelineInCanvas(pipelineId) {
    const data = await apiRequest(`/api/pipelines/${pipelineId}`);
    if (!data || !data.success) return;

    const pipeline = data.pipeline;
    canvasNodes = pipeline.nodes || [];
    canvasConnections = (pipeline.connections || []).map(c => ({
        ...c,
        cardinality: c.cardinality || '1:1',
        tableMappings: c.tableMappings || [],
    }));
    selectedNode = null;
    selectedConnection = null;
    nodeIdCounter = canvasNodes.reduce((max, n) => {
        const num = parseInt(n.id.replace('node_', ''), 10);
        return isNaN(num) ? max : Math.max(max, num);
    }, 0);
    
    switchTab('canvas');
    setTimeout(() => {
        renderCanvas();
        renderInspector();
        showToast('流水线已加载到画布', 'success');
    }, 100);
}

async function startPipeline(pipelineId) {
    if (!confirm('确定要启动此流水线吗？')) return;

    showToast('正在启动同步任务...', 'info');
    const data = await apiRequest(`/api/pipelines/${pipelineId}/start`, 'POST', { mode: 'resume' });
    if (data && data.success) {
        showToast('同步任务已启动', 'success');
        setTimeout(() => {
            switchTab('tasks');
        }, 1000);
    } else {
        showToast(data?.message || '启动失败', 'error');
    }
}

async function deletePipeline(pipelineId) {
    if (!confirm('确定要删除此流水线吗？删除后将无法恢复。')) return;

    const data = await apiRequest(`/api/pipelines/${pipelineId}`, 'DELETE');
    if (data && data.success) {
        showToast('流水线删除成功', 'success');
        loadPipelines();
    }
}

// ====================================================================== //
//                           快速开始向导
// ====================================================================== //

function showQuickStart() {
    document.getElementById('quickStartModal').classList.add('active');
}

function closeQuickStartModal() {
    document.getElementById('quickStartModal').classList.remove('active');
}

// ====================================================================== //
//                           更新仪表盘加载
// ====================================================================== //

async function loadDashboard() {
    const data = await apiRequest('/api/dashboard');
    if (!data || !data.success) return;

    const stats = data.stats;
    
    const statResources = document.getElementById('statResources');
    const statPipelines = document.getElementById('statPipelines');
    
    if (statResources) statResources.textContent = stats.total_resources || 0;
    if (statPipelines) statPipelines.textContent = stats.total_pipelines || 0;
    
    const statRunning = document.getElementById('statRunning');
    if (statRunning) statRunning.textContent = stats.running_tasks || 0;
    
    const statSyncedRows = document.getElementById('statSyncedRows');
    const statSyncedRows2 = document.getElementById('statSyncedRows2');
    if (statSyncedRows) statSyncedRows.textContent = formatNumber(stats.synced_rows || 0);
    if (statSyncedRows2) statSyncedRows2.textContent = formatNumber(stats.synced_rows || 0);

    const percentage = stats.total_rows > 0 ? Math.round((stats.synced_rows / stats.total_rows) * 100) : 0;
    const overallPercentage = document.getElementById('overallPercentage');
    if (overallPercentage) {
        overallPercentage.textContent = percentage + '%';
        updateProgressRing(percentage);
    }
    
    const completedTables = document.getElementById('statCompletedTables');
    const totalTables = document.getElementById('statTotalTables');
    const totalRows = document.getElementById('statTotalRows');
    
    if (completedTables) completedTables.textContent = stats.completed_tables || 0;
    if (totalTables) totalTables.textContent = stats.total_tables_in_progress || 0;
    if (totalRows) totalRows.textContent = formatNumber(stats.total_rows || 0);

    renderRecentActivity(data.recent_tasks);
}

function renderRecentActivity(tasks) {
    const container = document.getElementById('recentActivityList');
    
    if (!tasks || tasks.length === 0) {
        container.innerHTML = `
            <div class="empty-state-small">
                <p>暂无活动记录</p>
            </div>
        `;
        return;
    }

    container.innerHTML = tasks.map(task => `
        <div class="activity-item" onclick="viewTaskDetail('${task.task_id}')">
            <div class="activity-icon ${task.status}">
                ${task.status === 'completed' ? '✓' : task.status === 'running' ? '▶' : task.status === 'failed' ? '✕' : '○'}
            </div>
            <div class="activity-content">
                <div class="activity-title">${task.config_name}</div>
                <div class="activity-meta">${getModeLabel(task.mode)} · ${formatDate(task.created_at)}</div>
            </div>
            ${getStatusBadge(task.status)}
        </div>
    `).join('');
}

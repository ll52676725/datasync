document.addEventListener('DOMContentLoaded', function() {
    const loginForm = document.getElementById('loginForm');
    const loginBtn = document.getElementById('loginBtn');
    const errorMessage = document.getElementById('errorMessage');
    const btnText = loginBtn.querySelector('.btn-text');
    const btnLoading = loginBtn.querySelector('.btn-loading');

    const savedUsername = localStorage.getItem('sync_username');
    if (savedUsername) {
        document.getElementById('username').value = savedUsername;
        document.getElementById('remember').checked = true;
    }

    loginForm.addEventListener('submit', async function(e) {
        e.preventDefault();
        
        const username = document.getElementById('username').value.trim();
        const password = document.getElementById('password').value.trim();
        const remember = document.getElementById('remember').checked;

        if (!username || !password) {
            showError('请输入用户名和密码');
            return;
        }

        setLoading(true);
        hideError();

        try {
            const response = await fetch('/login', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify({ username, password })
            });

            const data = await response.json();

            if (data.success) {
                if (remember) {
                    localStorage.setItem('sync_username', username);
                } else {
                    localStorage.removeItem('sync_username');
                }
                
                showToast('登录成功', 'success');
                setTimeout(() => {
                    window.location.href = data.redirect || '/';
                }, 500);
            } else {
                showError(data.message || '登录失败');
            }
        } catch (error) {
            console.error('Login error:', error);
            showError('网络错误，请稍后重试');
        } finally {
            setLoading(false);
        }
    });

    function setLoading(loading) {
        loginBtn.disabled = loading;
        if (loading) {
            btnText.style.display = 'none';
            btnLoading.style.display = 'inline-flex';
        } else {
            btnText.style.display = 'inline';
            btnLoading.style.display = 'none';
        }
    }

    function showError(message) {
        errorMessage.textContent = message;
        errorMessage.style.display = 'block';
    }

    function hideError() {
        errorMessage.style.display = 'none';
    }

    function showToast(message, type = 'info') {
        const toast = document.createElement('div');
        toast.className = `toast ${type} show`;
        toast.textContent = message;
        document.body.appendChild(toast);
        
        setTimeout(() => {
            toast.classList.remove('show');
            setTimeout(() => toast.remove(), 300);
        }, 2000);
    }

    document.querySelectorAll('input').forEach(input => {
        input.addEventListener('keypress', function(e) {
            if (e.key === 'Enter') {
                loginForm.dispatchEvent(new Event('submit'));
            }
        });
    });
});

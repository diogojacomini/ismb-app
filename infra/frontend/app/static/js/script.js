document.addEventListener('DOMContentLoaded', function() {
    const btn = document.getElementById('hello-btn');
    if (btn) {
        btn.addEventListener('click', function() {
            alert('Olá, GitHub Pages!');
        });
    }
});

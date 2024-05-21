function placeImagesRandomly() {
    const container = document.querySelector('.split-design');
    const images = container.querySelectorAll('img');

    images.forEach(img => {
        const maxX = container.clientWidth - img.clientWidth;
        const maxY = container.clientHeight - img.clientHeight;
        const randomX = Math.floor(Math.random() * maxX);
        const randomY = Math.floor(Math.random() * maxY);
        img.style.left = randomX + 'px';
        img.style.top = randomY + 'px';
    });
}

window.onload = placeImagesRandomly;

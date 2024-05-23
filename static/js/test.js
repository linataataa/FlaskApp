function placeImagesRandomly() {
    const container = document.querySelector('.split-design');
 //   const images = container.querySelectorAll('img');

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

function showday(){
    console.log("day show");
    let div=document.getElementById("Jour-call")
    let eve=document.getElementById("eve-call")
    let night=document.getElementById("night-call")

    div.style.display = "block";
    eve.style.display = "none";
    night.style.display = "none";


}

function showeve(){
    console.log("eve show");
    let div=document.getElementById("eve-call")
    let day=document.getElementById("Jour-call")
    let night=document.getElementById("night-call")

    div.style.display = "block";
    day.style.display = "none";
    night.style.display = "none";


}
function shownight(){
    console.log("night show");
    let div=document.getElementById("night-call")
    let day=document.getElementById("Jour-call")
    let eve=document.getElementById("eve-call")

    div.style.display = "block";
    day.style.display = "none";
    eve.style.display = "none";


}

function services(){
    let ser=document.getElementById("Services")
    let per=document.getElementById("personnel")
    let pri=document.getElementById("prediction")

    ser.style.display = "block";
    per.style.display = "none";

    pri.style.display = "none";

}

function personnel(){
    let ser=document.getElementById("Services")
    let per=document.getElementById("personnel")
    let pri=document.getElementById("prediction")


    ser.style.display = "none";
    per.style.display = "block";
    pri.style.display = "none";



}
function prediction(){
    let ser=document.getElementById("Services")
    let per=document.getElementById("personnel")
    let pri=document.getElementById("prediction")


    ser.style.display = "none";
    per.style.display = "none";
    pri.style.display = "block";


}
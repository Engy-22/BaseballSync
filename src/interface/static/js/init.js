function init() {
    $('.team_pic').bind('click', function() {
        $( this ).toggleClass('selected_team unselected_team'); //change selected team to pop out
        return false;
    });
}

window.addEventListener("load", init);

function init() {
    $('.team_pic').bind('click', function() {
        $( this ).toggleClass('team_pic selected_team'); //change selected team to pop out or sink in
        return false;
    });
//    $('.team_label').bind('click', function() {
//        $( '.team_pic' ).toggleClass('selected_team team_pic'); //change selected team to pop out
//        return false;
//    });
}

window.addEventListener("load", init);

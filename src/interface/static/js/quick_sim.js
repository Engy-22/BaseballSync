$(document).ready(function() {
    $('#matchup').hide();
    $('.team-entity').on('click', function() {
        $(this).find('img').toggleClass('unselected_team selected_team'); //change selected team to be highlighted or unhighlighted
        if ($('.selected_team').length == 2) {
            $(this).find('img').attr('id', 'home_team');
            $('.unselected_team').parent('figure').addClass('hide-figure');
            hide_table();
        } else {
            $('.unselected_team').parent('figure').removeClass('hide-figure');
            if ($('.selected_team').length == 1) {
                $(this).find('img').attr('id', 'away_team');
            }
        } //end if-else
        $('.unselected_team').attr('id', '');
        return false;
    }); //end click event function
}); //end document ready function


function hide_table() {
    $('.simulate-content-section').fadeOut(function() { //hide all teams that were not selected
        display_matchup();
        return false;
    });
};


function display_matchup() {
    document.getElementById('away_matchup').src = $('#away_team').attr('src');
    document.getElementById('home_matchup').src = $('#home_team').attr('src');
    $('#matchup').removeClass('hidden');
    $('#matchup').fadeIn(750);
}; //end move_teams function


function show_table() {
    $('.simulate-content-section').fadeIn(function() { //show all teams that were previously hidden
        remove_matchup();
    });
};


function remove_matchup() {
    $('#matchup').fadeOut(750);
}; //end move_teams function

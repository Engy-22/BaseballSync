$(document).ready(function() {

    $('#matchup').hide();

    $('.remove_team').click(function() {
        remove_matchup();
        var this_team = $(this).parent().parent().parent().find('img').attr('id').split('_', 1)[0];
        $('#' + this_team + '_team').attr('id', '')
                                    .attr('class', 'unselected_team');
    }); //end remove_team click function

    $('.swap-button').click(function() {
        $('#away_team').attr('id', 'temp_team');
        $('#home_team').attr('id', 'away_team');
        $('#temp_team').attr('id', 'home_team');
        $('#away_matchup').fadeOut();
        $('#home_matchup').fadeOut(function() {
            var temp_src = $('#away_matchup').attr('src');
            $('#away_matchup').attr('src', $('#home_matchup').attr('src'));
            $('#home_matchup').attr('src', temp_src);
            $('#away_matchup').fadeIn();
            $('#home_matchup').fadeIn();
            $('.swap-button').blur();
        }); //end anonymous function
    }); //end swap-button click function

    $('.team-entity').on('click', function() {
        $(this).find('img').toggleClass('unselected_team selected_team'); //change selected team to be highlighted or unhighlighted
        if ($('.selected_team').length == 2) {
            if ($('#home_team').length > 0) {
                $(this).find('img').attr('id', 'away_team');
            } else {
                $(this).find('img').attr('id', 'home_team');
            } //end if-else
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
    $('.simulate-content-section').fadeOut(750, function() { //hide all teams that were not selected
        display_matchup();
        return false;
    });
};


function display_matchup() {
    $('#away_matchup').attr('src', $('#away_team').attr('src'));
    $('#home_matchup').attr('src', $('#home_team').attr('src'));
    $('#matchup').removeClass('hidden')
                 .fadeIn(750); //show the matchup of the two selected teams
};


function show_table() {
    $('.simulate-content-section').fadeIn(750); //show the entire league table again
};


function remove_matchup() {
    $('#matchup').fadeOut(750, function() { //hide the matchup
        show_table();
    });
};

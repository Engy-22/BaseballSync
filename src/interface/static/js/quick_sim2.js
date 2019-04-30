$(document).ready(function() {

    $('#matchup').hide();

    $('.year_in_dropdown').click(function() {
        var new_year = $(this).text()
        $('#years_dropdown').text(new_year);
        $.post("/simulate/quick_sim",
            {"newest_year": new_year},
            function(data) {display_new_teams(data);} // display the teams for the year returned
        ); //end ajax post request
    }); //end year dropdown onclick event

    $('.remove_team').click(function() {
        remove_matchup();
        var this_team = $(this).parent().parent().parent().find('img').attr('id').split('_')[0];
        $('#' + this_team + '_team').attr('id', '')
                                    .attr('class', 'unselected_team mt-5 ml-4');
        show_table();
    }); //end remove_team click function

    $('.swap-button').click(function() {
        $('#away_team').attr('id', 'temp_team');
        $('#home_team').attr('id', 'away_team');
        $('#temp_team').attr('id', 'home_team');
        $('#away_caption').fadeOut();
        $('#home_caption').fadeOut();
        $('#away_matchup').fadeOut();
        $('#home_matchup').fadeOut(function() {
            var temp_src = $('#away_matchup').attr('src');
            $('#away_matchup').attr('src', $('#home_matchup').attr('src'));
            $('#home_matchup').attr('src', temp_src);
            var temp_caption = $('#home_caption').text();
            $('#home_caption').text($('#away_caption').text());
            $('#away_caption').text(temp_caption);
            $('#away_matchup').fadeIn();
            $('#home_matchup').fadeIn();
            $('#home_caption').fadeIn();
            $('#away_caption').fadeIn();
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
    $('.content-section').fadeOut('slow', function() { //hide the entire grid
        display_matchup();
        return false;
    });
};


function display_matchup() {
    $('#away_caption').html($('#away_team').parent().find('figcaption').text());
    $('#home_caption').html($('#home_team').parent().find('figcaption').text());
    $('#away_matchup').attr('src', $('#away_team').attr('src'));
    $('#home_matchup').attr('src', $('#home_team').attr('src'));
    $('#matchup').removeClass('hidden')
                 .fadeIn(750); //show the matchup of the two selected teams
};


function show_table() {
    $('.content-section').fadeIn(750); //show the entire league table again
};


function remove_matchup() {
    $('#matchup').fadeOut(750, function() { //hide the matchup
        show_table();
    });
};


function display_new_teams(league_structure) { //display the grid of teams given any particular year
    var text = '<div class="row" id="league-names-row">';
    var json = JSON.parse(league_structure);
    Object.keys(json['new_year']).forEach(function(league) {
        text += '<div class="col-lg-' + 12/json['league_len'].toString() + ' text-center"><img class="league_pic" src="../static/images/model/leagues/' + league + '.png"/><div class="row mt-4" id="division-names-row">';
        Object.keys(json['new_year'][league]).forEach(function(division) {
            text += '<div class="col-md-' + 12/json['division_len'].toString() + '"><a class="division-header" href="#">' + division + '</a><div class="row">';
            Object.keys(json['new_year'][league][division]).forEach(function(team) {
                text += '<a href="#"><figure class="team-entity">';
                text += '<img class="unselected_team mt-5 ml-4" alt="' + json['new_year'][league][division][team] + '" src="../static/images/model/teams/' + team + json['year'] + '.jpg"/>';
                text += '<figcaption class="team_label mt-2 ml-4">' + json['new_year'][league][division][team] + '</figcaption></figure></a>';
            }); //end teams in division for loop
            text += '</div>';
            text += '</div>';
        }); //end divisions in league for loop
        text += '</div>';
        text += '</div>';
    }); //end leagues in MLB for loop
    text += '</div>';
    $('.teams_grid').html(text);
}; //end display_new_teams function

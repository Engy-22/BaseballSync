$(document).ready(function() {

    $('#matchup').hide();

    $(document).on('click', '.year_in_dropdown', function() {
        var new_year = $(this).text()
        $('#years_dropdown').text(new_year);
        $.post("/simulate/quick_sim",
            {"newest_year": new_year},
            function(data) {display_new_teams(data);} // display the teams for the year returned
        ); //end ajax post request
        return false;
    }); //end year dropdown onclick event

    $(document).on('click', '.remove_team', function() {
        remove_matchup();
        var this_team = $(this).parent().parent().parent().find('img').attr('id').split('_')[0];
        $('#' + this_team + '_team').attr('id', '')
                                    .attr('class', 'unselected_team mt-5 ml-4');
        show_table();
    }); //end remove_team onclick function

    $(document).on('click', '.swap-button', function() {
        $('#away_team').attr('id', 'temp_team');
        $('#home_team').attr('id', 'away_team');
        $('#temp_team').attr('id', 'home_team');
        $('#away_caption, #home_caption, #away_matchup').fadeOut();
        $('#home_matchup').fadeOut(function() {
            var temp_src = $('#away_matchup').attr('src');
            $('#away_matchup').attr('src', $('#home_matchup').attr('src'));
            $('#home_matchup').attr('src', temp_src);
            var temp_caption = $('#home_caption').text();
            $('#home_caption').text($('#away_caption').text());
            $('#away_caption').text(temp_caption);
            $('#away_matchup, #home_matchup, #home_caption, #away_caption').fadeIn();
            $('.swap-button').blur();
        }); //end anonymous function
    }); //end swap-button onclick function


    $(document).on('click', '.team-entity', function() {
        $(this).find('img').toggleClass('unselected_team selected_team'); //change selected team to be highlighted or unhighlighted
        if ($('#away_contender_year').length > 0) {
            $(this).find('img').attr('id', 'home_team');
            display_contender($(this).find('figcaption').html(), 'home_contender_year', $('#years_dropdown').text(), $(this).find('img').attr('src'));
        } else {
            $(this).find('img').attr('id', 'away_team');
            display_contender($(this).find('figcaption').html(), 'away_contender_year', $('#years_dropdown').text(), $(this).find('img').attr('src'));
        } //end if-else
        $('.unselected_team').attr('id', '');
        if ($('#away_contender_year').text().length > 0 && $('#home_contender_year').text().length > 0) {
            hide_table();
        }
        return false;
    }); //end onclick event function
}); //end document ready function


function hide_table() {
    $('.content-section').fadeOut('slow', function() { //hide the entire grid
        display_matchup(); //display the matchup
        return false;
    });
}; //end hide_table function


function display_matchup() {
    $('#away_caption').html($('#away_contender_year').text().split(";")[0]);
    $('#home_caption').html($('#home_contender_year').text().split(";")[0]);
    $('#away_matchup').attr('src', $('#away_contender_year').text().split(";")[1]);
    $('#home_matchup').attr('src', $('#home_contender_year').text().split(";")[1]);
    $('#matchup').removeClass('hidden')
                 .fadeIn(750); //show the matchup of the two selected teams
}; //end display_matchup function


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
            if (division != 'None') {
                text += '<div class="col-md-' + 12/json['division_len'].toString() + '"><a class="division-header" href="#">' + division + '</a><div class="row">';
            } else {
                text += '<div class="col-md-' + 12/json['division_len'].toString() + '"><div class="row">'
            } //end if-branch
            Object.keys(json['new_year'][league][division]).forEach(function(team) {
                text += '<a href="#"><figure class="team-entity">';
                text += '<img class="unselected_team mt-5 ml-' + 12/json['division_len'].toString() + '" alt="' + json['new_year'][league][division][team] + '" src="../static/images/model/teams/' + team + json['year'] + '.jpg"/>';
                text += '<figcaption class="team_label mt-2 ml-' + 12/json['division_len'].toString() + '">' + json['new_year'][league][division][team] + '</figcaption></figure></a>';
            }); //end teams in division for loop
            text += '</div>';
            text += '</div>';
        }); //end divisions in league for loop
        text += '</div>';
        text += '</div>';
    }); //end leagues in MLB for loop
    text += '</div>';
    $('.teams_grid').html(text);

    $('img').on("error", function() {
      $(this).attr('src', '../static/images/emblem.png');
      $(this).addClass('emblem-default');
      $(this).parent().addClass('shrink-anchor');
    });

    return false;
}; //end display_new_teams function


function display_contender(team_name, home_away, year, img_src) {
    $('#' + home_away + '_column').hide();
    $('#' + home_away + '_column').find('div').attr('id', home_away);
    $('#' + home_away + '_column').find('div').html(team_name + ' ' + year + ';' + img_src);
}; //end display_contender function


function remove_contender(team, home_away) {
    $('#' + home_away).html('');
    $('#' + home_away + '_contender').find('div').attr('id', '');
}; //end remove_contender function

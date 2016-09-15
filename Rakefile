require 'pathname'
DATA_DIR = Pathname 'catalog'
WRANGLE_DIR = Pathname 'wrangle'
CORRAL_DIR = WRANGLE_DIR / 'corral'
SCRIPTS = WRANGLE_DIR / 'scripts'
DIRS = {
    'fetched' => CORRAL_DIR / 'fetched',
    'compiled' => CORRAL_DIR / 'compiled',
    'published' => DATA_DIR,
}


START_YEAR = 2009
END_YEAR = 2016


F_FILES = (START_YEAR..END_YEAR).inject({}){|h, yr| h[yr.to_s] = DIRS['fetched'] / "#{yr}.csv"; h }
C_FILES = (START_YEAR..END_YEAR).inject({}){|h, yr| h[yr.to_s] = DIRS['compiled'] / "#{yr}.csv"; h }
P_FILES = (START_YEAR..END_YEAR).inject({}){|h, yr| h[yr.to_s] = DIRS['published'] / "sf-311-cases-#{yr}.csv"; h }



desc 'Setup the directories'
task :setup do
    DIRS.each_value do |p|
        unless p.exist?
            p.mkpath()
            puts "Created directory: #{p}"
        end
    end
end


desc "Fetch everything"
task :fetch  => [:setup] do
  F_FILES.each_value{|fn| Rake::Task[fn].execute() }
end

desc "Compile everything"
task :compile  => [:setup] do
  C_FILES.each_value{|fn| Rake::Task[fn].execute() }
end

desc "publish everything"
task :publish  => [:setup] do
  P_FILES.each_value{|fn| Rake::Task[fn].execute() }
end

desc "Pull newest year data, recompile and republish"
task :refresh do
    # TK TODO: END_YEAR should be changed to dynamically be THIS year
    thisyear = END_YEAR.to_s
    Rake::Task[F_FILES[thisyear]].execute()
end





namespace :files do
  namespace :published do
    P_FILES.each_pair do |year, fname|
      desc "Publish year #{year}"
      srcname = C_FILES[year.to_s]
      file fname => srcname do
        sh "cp #{srcname} #{fname}"
      end
    end
  end


  namespace :compiled do
    C_FILES.each_pair do |year, fname|
      desc "Compile year #{year}"
      srcname = F_FILES[year.to_s]
      file fname => srcname do
        sh "python #{SCRIPTS.join('clean.py')} #{srcname} > #{fname}"

      end
    end
  end

  namespace :fetched do
    F_FILES.each_pair do |year, fname|
        desc "Fetch year #{year}"
        file fname => :setup do
            sh "python #{SCRIPTS.join('fetch_year.py')} #{year} > #{fname}"
        end
    end
  end
end

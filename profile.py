'''
to create specific profiles to admin systems
'''

class Profile:
    def __init__(self,admin_system=None):
        if not admin_system:
            self.steps_required=[True for i in range(10)]
        if  admin_system.lower()=='as400':
            self.steps_required=[True for i in range(10)]
        